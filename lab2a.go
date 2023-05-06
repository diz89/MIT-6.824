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

func (rf *Raft) getTermSafe() int {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getServerStateSafe() ServerState {
	rf.Lock()
	defer rf.Unlock()
	return rf.serverState
}

func (rf *Raft) sendHeartbeat(term int) { // Unsafe, not with lock
	for i := 0; i < len(rf.peers); i++ {
		if (i == rf.me) {
			continue;
		}
		go func(server int) {
			if rf.getServerStateSafe() != Leader  || rf.getTermSafe() != term {
				return 
			}
			args := AppendEntryArgs{Term: term, LeaderId: rf.me}
			reply := AppendEntryReply{}
			ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
			if !ok {
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
			// 2B
		}(i)
	}	
}

func (rf *Raft) runAsLeader() { // Unsafe
	go rf.sendHeartbeat(rf.currentTerm)
	rf.startNewTimer(getLeaderHeartbeatIntervalMs(), func() {
		rf.runAsLeader()
	})
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
		rf.serverState = Leader
		rf.runAsLeader()
	}
}

func (rf *Raft) refreshTerm(term int) { // unsafe, needs lock wrapped
	DPrintf("Server %d (%d) refresh term %d -> %d\n", rf.me, rf.serverState, rf.currentTerm, term)
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

func (rf *Raft) electionTimedOut() { // unsafe, needs lock wrapped
	DPrintf("Server %d (%d) election timed out, currentTerm = %d\n", rf.me, rf.serverState, rf.currentTerm)

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
			if rf.getServerStateSafe() != Candidate  || rf.getTermSafe() != term {
				return 
			}
			args := RequestVoteArgs{Term: term, CandidateId: rf.me}
			reply := RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if (!ok) {
				return
			}
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
		}(i)
	}

}

func (rf *Raft) startNewTimer(timeoutMs int64, callBack CallBackFunc) { // unsafe, needs to be wrapped with Lock
	if (rf.lastTimer != nil) {
		rf.lastTimer.callBack = emptyCallBack
		rf.lastTimer.cleared = true
	}
	newTimer := Timer {timeoutMs, callBack, rf.timerId, false}
	DPrintf("Start New Timer: Server %d id %d (term = %d state %d) timeout %d\n", rf.me, newTimer.timerId, rf.currentTerm, rf.serverState, newTimer.timeoutMs)
	rf.timerId++
	rf.lastTimer = &newTimer
	go func() {
		time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
		rf.Lock()
		defer rf.Unlock()
		DPrintf("Timer ellapsed: Server %d id %d (term = %d state %d) timeout = %d cleared = %v\n", rf.me, newTimer.timerId, rf.currentTerm, rf.serverState, newTimer.timeoutMs, newTimer.cleared)
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
	// Your code here (2A, 2B).
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
}

type AppendEntryReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	DPrintf("Server %d term %d state %d receive AppendEntry args = (%d, %d)\n", rf.me, rf.currentTerm, rf.serverState, args.Term, args.LeaderId)

	if args.Term > rf.currentTerm {
		rf.refreshTerm(args.Term)
	} else if args.Term == rf.currentTerm  {
		if rf.serverState == Leader {
			// error
		} else {
			rf.refreshTerm(args.Term)
		}
	} else {
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func getElectionTimeoutMs() int64 {
	return 500 + (rand.Int63() % 500);
}

func getLeaderHeartbeatIntervalMs() int64 {
	return 30
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Lock()
	defer rf.Unlock()
	rf.currentTerm = 0
	rf.serverState = Follower
	rf.votedFor = -1
	rf.startNewTimer(getElectionTimeoutMs(), func() {
		rf.electionTimedOut()
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
