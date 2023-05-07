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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
const heartbeatIntervalMs int = 40

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
	// CommandIndex int
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

	initialized bool
	// Used for debug
	timerId int

	lastReplicateEntriesTime []time.Time
	maxLogEntriesPerRpc []int

	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm int
	// snapshotLastIncludedCommandIndex int
	snapshot []byte
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
	if !ok || rf.killed(){
		return
	}
	DPrintf("Server %d calls AppendEntry RPC to server %d, args = %v receive reply: %v", rf.me, server, args, reply)

	rf.Lock()
	defer rf.Unlock()
	if reply.Term > rf.currentTerm {
		rf.refreshTerm(reply.Term)
		rf.persist()
	}
	if rf.currentTerm != args.Term || rf.serverState != Leader {
		return
	}
	if rf.nextIndex[server] != args.PrevLogIndex + 1 {
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		if rf.maxLogEntriesPerRpc[server] < 1000 {
			rf.maxLogEntriesPerRpc[server] *= 2
		}
	} else {
		rf.nextIndex[server] = args.PrevLogIndex
		if reply.LastLogIndex < args.PrevLogIndex {
			rf.nextIndex[server] = min(rf.nextIndex[server], reply.LastLogIndex + 1)
		} else {
			if rf.snapshotLastIncludedIndex < args.PrevLogIndex {
				index := rf.findFirstIndexWithTermGreaterOrEqualTo(reply.ConflictingTerm + 1,
					rf.snapshotLastIncludedIndex,
					args.PrevLogIndex) - 1
				if rf.getTermAt(index) == reply.ConflictingTerm {
					rf.nextIndex[server] = min(rf.nextIndex[server], index + 1)
				} else {
					rf.nextIndex[server] = min(rf.nextIndex[server], reply.ConflictingTermFirstIndex)
				}
			}
		}
		rf.maxLogEntriesPerRpc[server] = 1
	}

	DPrintf("Server %d calls AppendEntry RPC to server %d, args = %v receive reply: %v, matchIndex->%d, nextIndex->%d\n", rf.me, server, args, reply, rf.matchIndex[server], rf.nextIndex[server])
}

func (rf *Raft) callInstallSnapshotRpcSafe(server int, args InstallSnapshotArgs, currentNextIndex int) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok || rf.killed(){
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if reply.Term > rf.currentTerm {
		rf.refreshTerm(reply.Term)
		rf.persist()
	}
	if rf.currentTerm != args.Term || rf.serverState != Leader {
		return
	}
	if rf.nextIndex[server] != currentNextIndex {
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		if reply.GreaterLastIncludedIndex > 0 {
			rf.matchIndex[server] = reply.GreaterLastIncludedIndex
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
	}
}

func (rf *Raft) becomeLeader() { // Unsafe
	rf.serverState = Leader
	DPrintf("Server %d becomes leader, term %d\n", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
		rf.maxLogEntriesPerRpc[i] = 1
		go rf.leaderReplicateFollowerSafe(i)
	}
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
}

func (rf *Raft) resetTimerForElectionTimeout() { // unsafe, needs lock wrapped
	rf.startNewTimer(getElectionTimeoutMs(), rf.electionTimedOut)
}

func (rf *Raft) electionTimedOut() { // unsafe, needs lock wrapped
	if rf.killed() {
		return
	}

	//DPrintf("Server %d election timed out, currentTerm = %d, state = %v\n", rf.me, rf.currentTerm, rf.serverState)
	rf.resetTimerForElectionTimeout()
	if rf.serverState == Leader {
		return
	}
	
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.receivedVoteCount = 1
	rf.persist()
	DPrintf("Server %d starts election, term = %d\n", rf.me, rf.currentTerm)
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
				LastLogTerm: rf.getLastLogTerm(),
			}
			go func() {
				reply := RequestVoteReply{}
				//DPrintf("Server %d call RequestVote RPC to server %d, args = %v ", rf.me, server, args)
				ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
				if (!ok) {
					return
				}
				//DPrintf("Server %d call RequestVote RPC to server %d, args = %v receive reply: %v", rf.me, server, args, reply)

				if reply.Term > term {
					rf.Lock()
					defer rf.Unlock()
					if reply.Term > rf.currentTerm {
						rf.refreshTerm(reply.Term)
						rf.persist()
						//rf.resetTimerForElectionTimeout()
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
func (rf *Raft) persist() { // Needs locks wrapped
	// Your code here (2C).
    w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.logs)

	encoder.Encode(rf.snapshotLastIncludedIndex)
	encoder.Encode(rf.snapshotLastIncludedTerm)
	// encoder.Encode(rf.snapshotLastIncludedCommandIndex)

	rf.persister.Save(w.Bytes(), rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var snapshotLastIncludedIndex int
	var snapshotLastIncludedTerm int
	//var snapshotLastIncludedCommandIndex int
	
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&snapshotLastIncludedIndex) != nil ||
		decoder.Decode(&snapshotLastIncludedTerm) != nil {
			/*|| decoder.Decode(&snapshotLastIncludedCommandIndex) != nil*/
	   DPrintf("[ERROR] Server %d cannot read persisted data\n", rf.me);
	} else {
		rf.Lock()
		defer rf.Unlock()
	    rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
		rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
		//rf.snapshotLastIncludedCommandIndex = snapshotLastIncludedCommandIndex
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.Lock()
	defer rf.Unlock()
	if index <= rf.snapshotLastIncludedIndex {
		return
	}

	lastIncludedTerm := rf.getTermAt(index)

	rf.removeLogEndingWith(index)
	rf.snapshot = snapshot
	rf.snapshotLastIncludedIndex = index
	rf.snapshotLastIncludedTerm = lastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, rf.snapshotLastIncludedIndex)
	rf.persist()
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
	if !rf.initialized {
		return
	}

	//DPrintf("Server %d term %d state %d receive RequestVote args = (%d, %d)\n", rf.me, rf.currentTerm, rf.serverState, args.Term, args.CandidateId)

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
		needPersist = true
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		needPersist = true
		reply.VoteGranted = true
		rf.resetTimerForElectionTimeout()
		return
	}
	// Your code here (2A, 2B).
}

func (rf* Raft) isCandidateUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool { // unsafe
	return candidateLastLogTerm > rf.getLastLogTerm() || candidateLastLogTerm == rf.getLastLogTerm() && candidateLastLogIndex >= rf.getLastLogIndex()
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

	ConflictingTerm int // term in the conflicting entry (if any)
    ConflictingTermFirstIndex int // index of first entry with that term (if any)
    LastLogIndex int
}

func (rf *Raft) findFirstIndexWithTermGreaterOrEqualTo(term int, l int, r int) int { // term of log at currentIndex is term
	for l + 1 < r {
		mid := (l + r) / 2
		if rf.getTermAt(mid) >= term {
			r = mid
		} else {
			l = mid
		}
	}
	return r
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	//DPrintf("Server %d term %d state %d receive AppendEntry args %v\n", rf.me, rf.currentTerm, rf.serverState, args)
	if !rf.initialized {
		return
	}

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
		rf.resetTimerForElectionTimeout()
		needPersist = true
	} else if args.Term == rf.currentTerm  {
		if rf.serverState == Leader {
			// error
		} else {
			rf.refreshTerm(args.Term)
			rf.resetTimerForElectionTimeout()
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.LastLogIndex = rf.getLastLogIndex()
		return		
	} else if args.PrevLogIndex < rf.snapshotLastIncludedIndex {
		// do nothing
	} else if prevLogTerm := rf.getTermAt(args.PrevLogIndex); prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		reply.LastLogIndex = rf.getLastLogIndex()
		reply.ConflictingTerm = prevLogTerm
		reply.ConflictingTermFirstIndex = rf.findFirstIndexWithTermGreaterOrEqualTo(prevLogTerm, rf.snapshotLastIncludedIndex,
			args.PrevLogIndex)
		return
	}

	reply.Success = true

	//DPrintf("Server %d term %d state %d receive AppendEntry args %v, original entries = %v\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs)

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it 
	for i := 0; i < len(args.Entries) && args.PrevLogIndex + 1 + i <= rf.getLastLogIndex(); i++ {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.snapshotLastIncludedIndex {
			continue
		}
		if rf.getTermAt(index) != args.Entries[i].Term {
			rf.removeLogStartingFrom(index)
			needPersist = true
			break
		}
	}

	//DPrintf("Server %d term %d state %d receive AppendEntry args %v, delete conflicted entries = %v\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs)	// Append any new entries not already in the log
	
	for i := 0; i < len(args.Entries); i++ {
		newLogIndex := args.PrevLogIndex + 1 + i
		if rf.getLastLogIndex() + 1 == newLogIndex {
			rf.logs = append(rf.logs, args.Entries[i])
			needPersist = true
		}
	}

	// If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries)))
	}

	//DPrintf("Server %d term %d state %d receive AppendEntry args %v, final entries = %v, commitIndex=%d\n", rf.me, rf.currentTerm, rf.serverState, args, rf.logs, rf.commitIndex)	// Append any new entries not already in the log

	return
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
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

	
	// commandIndex := rf.logs[rf.getLastLogIndex()].CommandIndex + 1
	commandIndex := rf.getLastLogIndex() + 1

	rf.logs = append(rf.logs, LogEntry {
		Command: command,
		// CommandIndex: commandIndex,
		Term: rf.currentTerm,
	})
	DPrintf("Start: Server %d, command = %v, commandIndex = %d, Term = %d\n", rf.me, command, commandIndex, rf.currentTerm)
	rf.persist()

	return commandIndex, rf.currentTerm, true
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
	return rf.snapshotLastIncludedIndex + len(rf.logs)
}

func (rf *Raft) getLastLogTerm() int { // not safe
	return rf.getTermAt(rf.getLastLogIndex())
}

func (rf *Raft) getTermAt(index int) int { // not safe
	if index < rf.snapshotLastIncludedIndex {
		panic(fmt.Sprintf("Server %d get Term at index = %d, snapshotLastIncludedIndex = %d", rf.me, index, rf.snapshotLastIncludedIndex))
	}
	if index == rf.snapshotLastIncludedIndex {
		return rf.snapshotLastIncludedTerm
	}
	return rf.logs[index - rf.snapshotLastIncludedIndex - 1].Term
}

func (rf *Raft) getLogAt(index int) LogEntry { // not safe
	if index <= rf.snapshotLastIncludedIndex {
		panic(fmt.Sprintf("Server %d get Log at index = %d, snapshotLastIncludedIndex = %d", rf.me, index, rf.snapshotLastIncludedIndex))
	}
	return rf.logs[index - rf.snapshotLastIncludedIndex - 1]
}

func (rf *Raft) removeLogStartingFrom(index int) { // unsafe, remove logs starting from index (includes index)
	rf.logs = rf.logs[0: index - rf.snapshotLastIncludedIndex - 1]
}

func (rf *Raft) removeLogEndingWith(index int) { // unsafe, remove logs ending with index (includes index)
	if index >= rf.getLastLogIndex() {
		rf.logs = make([]LogEntry, 0)
	} else {
		rf.logs = rf.logs[index - rf.snapshotLastIncludedIndex:]
	}	
}

func getElectionTimeoutMs() int64 {
	return 400 + (rand.Int63() % 400);
}

func (rf *Raft) getApplyMsgSafe() (ApplyMsg, bool) {
	rf.Lock()
	defer rf.Unlock()
	if rf.lastApplied + 1 <= rf.snapshotLastIncludedIndex {
		applyMsg := ApplyMsg {
			SnapshotValid: true,
			Snapshot: rf.snapshot,
			SnapshotTerm: rf.snapshotLastIncludedTerm,
			SnapshotIndex: rf.snapshotLastIncludedIndex,
		}
		rf.lastApplied = rf.snapshotLastIncludedIndex
		return applyMsg, true
	} else {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg {
				CommandValid: true,
				Command: rf.getLogAt(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied/* rf.logs[rf.lastApplied].CommandIndex*/,
			}
			return applyMsg, true
		} else {
			return ApplyMsg{}, false
		}
	}

}

func (rf *Raft) applyPeriodically() {
	for !rf.killed() {
		for {
			applyMsg, ok := rf.getApplyMsgSafe();
			if !ok {
				break
			}
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
	if rf.commitIndex < rf.snapshotLastIncludedIndex {
		rf.commitIndex = rf.snapshotLastIncludedIndex
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
		if rf.getTermAt(nextCommitIndex) == rf.currentTerm {
			rf.commitIndex = nextCommitIndex
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
	if time.Now().Sub(rf.lastReplicateEntriesTime[server]) < time.Duration(heartbeatIntervalMs) * time.Millisecond {
		return
	}

	// If last log index ≥ nextIndex for a follower: send
    // AppendEntries RPC with log entries starting at nextIndex
    // • If successful: update nextIndex and matchIndex for
    // follower (§5.3)
    // • If AppendEntries fails because of log inconsistency:
    // decrement nextIndex and retry (§5.3)

	// Sends at most 10 entries
	rf.lastReplicateEntriesTime[server] = time.Now()

	if rf.nextIndex[server] - 1 < rf.snapshotLastIncludedIndex {
		args := InstallSnapshotArgs {
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LastIncludedIndex: rf.snapshotLastIncludedIndex,
			LastIncludedTerm: rf.snapshotLastIncludedTerm,
			Snapshot: rf.snapshot,
		}

		go rf.callInstallSnapshotRpcSafe(server, args, rf.nextIndex[server])
	} else {
		entries := []LogEntry {}
		for i := 0; i < rf.maxLogEntriesPerRpc[server] && rf.nextIndex[server] + i <= rf.getLastLogIndex(); i++ {
			entries = append(entries, rf.getLogAt(rf.nextIndex[server] + i))
		}
		
		args := AppendEntryArgs {
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm: rf.getTermAt(rf.nextIndex[server] - 1),
			Entries: entries,
			LeaderCommit: rf.commitIndex,
		}
	
		go rf.callAppendEntryRpcSafe(server, args)
	}
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

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	// LastIncludedCommandIndex int
	LastIncludedTerm int
	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
	Success bool
	GreaterLastIncludedIndex int // If follower's last included index is greater
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	//DPrintf("Server %d term %d state %d receive AppendEntry args %v\n", rf.me, rf.currentTerm, rf.serverState, args)
	if !rf.initialized {
		return
	}

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
		rf.resetTimerForElectionTimeout()
		needPersist = true
	} else if args.Term == rf.currentTerm  {
		if rf.serverState == Leader {
			// error
		} else {
			rf.refreshTerm(args.Term)
			rf.resetTimerForElectionTimeout()
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.snapshotLastIncludedIndex {
		reply.Success = false
		reply.GreaterLastIncludedIndex = rf.snapshotLastIncludedIndex
		return
	}

	reply.Success = true
	//DPrintf("Server %d term %d state %d snapshotLastIncludedIndex = %d remove log ending with %d\n",
	//	rf.me, rf.currentTerm, rf.serverState, rf.snapshotLastIncludedIndex, args.LastIncludedIndex)
	rf.removeLogEndingWith(args.LastIncludedIndex)
	rf.snapshot = args.Snapshot
	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, rf.snapshotLastIncludedIndex)
	needPersist = true
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
		logs: make([]LogEntry, 0),
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		initialized: false,
		lastReplicateEntriesTime: make([]time.Time, len(peers)),
		maxLogEntriesPerRpc: make([]int, len(peers)),
		snapshot: persister.ReadSnapshot(),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).
	rf.Lock()
	defer rf.Unlock()

	rf.startNewTimer(getElectionTimeoutMs(), func() {
		rf.electionTimedOut()
	})

	go rf.applyPeriodically()
	go rf.leaderUpdateCommitIndexPeriodically()
	go rf.leaderReplicateFollowersPeriodically()

	rf.initialized = true
	return rf
}
