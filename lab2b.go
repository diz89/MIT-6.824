package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


type ServerState int

const (
    Follower ServerState = iota
    Candidate
	Leader
)

func (state ServerState) String() string {
	switch state {
	case Follower: return "Follower"
	case Candidate: return "Candidate"
	case Leader: return "Leader"
	default: return "Error"
	}
}

const repeatedCheckIntervalMs int = 10

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastTimer *Timer
	votedFor int
	currentTerm int
	serverState ServerState
	receivedVoteCount int
	
	applyCh chan ApplyMsg
	logs []LogEntry
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int

	// Used for debug
	timerId int
}

type CallBackFunc func()


type Timer struct {
  timeoutMs int64
  callBack CallBackFunc
  timerId int
  cleared bool
}

func emptyCallBack() {
	// do nothing
}

func (rf *Raft) callAppendEntryRpcSafe(server int, args AppendEntryArgs) { // should be called in a new goroutine
	DPrintf("Server %d calls AppendEntry RPC to server %d, args = %v", rf.me, server, args)
	reply := AppendEntryReply{}
	ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
	if !ok {
		return
	}
	DPrintf("Server %d calls AppendEntry RPC to server %d, args = %v receive reply: %v", rf.me, server, args, reply)

	rf.Lock()
	defer rf.Unlock()
	if reply.Term > rf.currentTerm {
		rf.refreshTerm(reply.Term)
	}
	if (rf.currentTerm != args.Term || rf.serverState != Leader) {
		return
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server]--
	}
}

func (rf *Raft) sendHeartbeat(term int) { // Unsafe, not with lock
	for i := 0; i < len(rf.peers); i++ {
		if (i == rf.me) {
			continue;
		}
		go func(server int) {
			// 2B
			rf.Lock()
			defer rf.Unlock()
			if rf.currentTerm != term || rf.serverState != Leader {
				return
			}
			
			args := AppendEntryArgs{
				Term: term,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm: rf.logs[rf.nextIndex[server] - 1].Term,
				Entries: []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}
			
			go rf.callAppendEntryRpcSafe(server, args)
		}(i)
	}
}

func (rf *Raft) runAsLeader() { // Unsafe
	if rf.killed() {
		return
	}
	go rf.sendHeartbeat(rf.currentTerm)
	rf.startNewTimer(getLeaderHeartbeatIntervalMs(), func() {
		rf.runAsLeader()
	})
}

func (rf *Raft) becomeLeader() { // Unsafe
	rf.serverState = Leader
	DPrintf("Server %d becomes leader, term %d\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.runAsLeader()
}

func (rf *Raft) receiveVoteSafe(term int) {
	rf.Lock()
	defer rf.Unlock()
	if rf.serverState != Candidate || rf.currentTerm != term {
		return
	}
	rf.receivedVoteCount++
	requiredVote := len(rf.peers) / 2 + 1
	if rf.receivedVoteCount == requiredVote {
		rf.becomeLeader()
	}
}

func (rf *Raft) refreshTerm(term int) { // unsafe, needs lock wrapped
	DPrintf("Server %d refreshes term %d -> %d, state = %v\n", rf.me, rf.currentTerm, term, rf.serverState)
	if rf.currentTerm > term {
		return
		// error
	} else if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.serverState = Follower
	} else {
		// candidate -> follower
		// follower -> follower
		rf.serverState = Follower
	}
	
	rf.startNewTimer(getElectionTimeoutMs(), rf.electionTimedOut)
}

func (rf *Raft) resetTimerForElectionTimeout() { // unsafe, needs lock wrapped
	rf.startNewTimer(getElectionTimeoutMs(), rf.electionTimedOut)
}

func (rf *Raft) electionTimedOut() { // unsafe, needs lock wrapped
	if rf.killed() {
		return
	}
	DPrintf("Server %d election timed out, currentTerm = %d, state = %v\n", rf.me, rf.currentTerm, rf.serverState)

	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.receivedVoteCount = 1
	rf.startNewTimer(getElectionTimeoutMs(), rf.electionTimedOut)
	go rf.StartRequestVotes(rf.currentTerm)
}

func (rf *Raft) StartRequestVotes(term int) {
	for i := 0; i < len(rf.peers); i++ {
		if (i == rf.me) {
			continue;
		}
		go func(server int) {
			rf.Lock()
			defer rf.Unlock()
			if rf.serverState != Candidate  || rf.currentTerm != term {
				return 
			}
			args := RequestVoteArgs {
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm: rf.logs[rf.getLastLogIndex()].Term,
			}
			go func() {
				reply := RequestVoteReply{}
				DPrintf("Server %d call RequestVote RPC to server %d, args = %v ", rf.me, server, args)
				ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				if (!ok) {
					return
				}
				DPrintf("Server %d call RequestVote RPC to server %d, args = %v receive reply: %v", rf.me, server, args, reply)
				if reply.Term < term {
					// error
					return
				}
				if reply.Term > term {
					rf.Lock()
					defer rf.Unlock()
					if reply.Term > rf.currentTerm {
						rf.refreshTerm(reply.Term)
					}	
					return
				}
				if reply.VoteGranted {
					rf.receiveVoteSafe(term)
				}
			}()

		}(i)
	}

}

func (rf *Raft) startNewTimer(timeoutMs int64, callBack CallBackFunc) { // unsafe, needs to be wrapped with Lock
	if (rf.lastTimer != nil) {
		rf.lastTimer.callBack = emptyCallBack
		rf.lastTimer.cleared = true
	}
	newTimer := Timer {timeoutMs, callBack, rf.timerId, false}
	// DPrintf("Start New Timer: Server %d id %d (term = %d state %d) timeout %d\n", rf.me, newTimer.timerId, rf.currentTerm, rf.serverState, newTimer.timeoutMs)
	rf.timerId++
	rf.lastTimer = &newTimer
	go func() {
		time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		rf.Lock()
		defer rf.Unlock()
		// DPrintf("Timer ellapsed: Server %d id %d (term = %d state %d) timeout = %d cleared = %v\n", rf.me, newTimer.timerId, rf.currentTerm, rf.serverState, newTimer.timeoutMs, newTimer.cleared)
		newTimer.callBack()
	}()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.serverState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()
	DPrintf("Server %d term %d state %d receive RequestVote args = (%d, %d)\n", rf.me, rf.currentTerm, rf.serverState, args.Term, args.CandidateId)

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimerForElectionTimeout()
		return
	}
	// Your code here (2A, 2B).
}

func (rf* Raft) isCandidateUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool { // unsafe
	lastLogIndex := rf.getLastLogIndex()

	return candidateLastLogTerm > rf.logs[lastLogIndex].Term || candidateLastLogTerm == rf.logs[lastLogIndex].Term && candidateLastLogIndex >= lastLogIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	DPrintf("Server %d term %d state %d receive AppendEntry args %v\n", rf.me, rf.currentTerm, rf.serverState, args)

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
	} else if args.Term == rf.currentTerm  {
		if rf.serverState == Leader {
			// error
		} else {
			rf.refreshTerm(args.Term)
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	DPrintf("Server %d term %d state %d receive AppendEntry args %v, original entries = %v\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs)

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it 
	for i := 0; i < len(args.Entries) && args.PrevLogIndex + 1 + i <= rf.getLastLogIndex(); i++ {
		if rf.logs[args.PrevLogIndex + 1 + i].Term != args.Entries[i].Term {
			rf.logs = rf.logs[:args.PrevLogIndex + 1 + i]
			break
		}
	}

	DPrintf("Server %d term %d state %d receive AppendEntry args %v, delete conflicted entries = %v\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs)	// Append any new entries not already in the log
	
	for i := 0; i < len(args.Entries); i++ {
		newLogIndex := args.PrevLogIndex + 1 + i
		if rf.getLastLogIndex() + 1 == newLogIndex {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}
	// If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
	}

	DPrintf("Server %d term %d state %d receive AppendEntry args %v, final entries = %v, commitIndex=%d\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs, rf.commitIndex)	// Append any new entries not already in the log

	return
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)

	rf.Lock()
	defer rf.Unlock()

	if rf.serverState != Leader {
		return -1, rf.currentTerm, false
	}

	rf.logs = append(rf.logs, LogEntry {
		Command: command,
		Term: rf.currentTerm,
	})

	return rf.getLastLogIndex(), rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastLogIndex() int { // not safe
	return len(rf.logs) - 1
}

func getElectionTimeoutMs() int64 {
	return 500 + (rand.Int63() % 500);
}

func getLeaderHeartbeatIntervalMs() int64 {
	return 50
}

func (rf *Raft) applyPeriodically() {
	for !rf.killed() {
		rf.Lock()
		commitIndex := rf.commitIndex
		rf.Unlock()
		for rf.lastApplied < commitIndex {
			rf.lastApplied++
			rf.Lock()
			applyMsg := ApplyMsg {
				CommandValid: true,
				Command: rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.Unlock()
			rf.applyCh <- applyMsg
		}
		time.Sleep(time.Duration(repeatedCheckIntervalMs) * time.Millisecond)
	}
}

func (rf *Raft) leaderUpdateCommitIndexSafe() {
	rf.Lock()
	defer rf.Unlock()
	if rf.serverState != Leader {
		return
	}
	for nextCommitIndex := rf.commitIndex + 1; nextCommitIndex <= rf.getLastLogIndex(); nextCommitIndex++ {
		cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me || rf.matchIndex[i] >= nextCommitIndex {
				cnt++
			}
		}
		if cnt <= len(rf.peers) / 2 {
			break
		}
		if rf.logs[nextCommitIndex].Term == rf.currentTerm {
			rf.commitIndex = nextCommitIndex
			break
		}
	}
}

func (rf *Raft) leaderUpdateCommitIndexPeriodically() {
	for !rf.killed() {
		rf.leaderUpdateCommitIndexSafe()
		time.Sleep(time.Duration(repeatedCheckIntervalMs) * time.Millisecond)
	}
}

func (rf *Raft) leaderReplicateFollowerSafe(server int) {
	rf.Lock()
	defer rf.Unlock()
	if rf.serverState != Leader {
		return
	}
	// If last log index ≥ nextIndex for a follower: send
    // AppendEntries RPC with log entries starting at nextIndex
    // • If successful: update nextIndex and matchIndex for
    // follower (§5.3)
    // • If AppendEntries fails because of log inconsistency:
    // decrement nextIndex and retry (§5.3)

	// Sends at most 3 entries

	entries := []LogEntry {}
	for i := 0; i < 3; i++ {
		if rf.nextIndex[server] + i <= rf.getLastLogIndex() {
			entries = append(entries, rf.logs[rf.nextIndex[server] + i])
		}
	}
	if len(entries) == 0 {
		return
	}
	args := AppendEntryArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm: rf.logs[rf.nextIndex[server] - 1].Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}

	go rf.callAppendEntryRpcSafe(server, args)
}

func (rf *Raft) leaderReplicateFollowersPeriodically() {
	for i := 0; i < len(rf.peers); i++ {
		if (i == rf.me) {
			continue;
		}
		go func(server int) {
			for !rf.killed() {
				rf.leaderReplicateFollowerSafe(server)
				time.Sleep(time.Duration(repeatedCheckIntervalMs) * time.Millisecond)
			}			
		}(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		applyCh: applyCh,
		logs: make([]LogEntry, 1),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.Lock()
	defer rf.Unlock()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.startNewTimer(getElectionTimeoutMs(), func() {
		rf.electionTimedOut()
	})

	go rf.applyPeriodically()
	go rf.leaderUpdateCommitIndexPeriodically()
	go rf.leaderReplicateFollowersPeriodically()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
