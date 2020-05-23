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

import "sync"
import "sync/atomic"
import "time"
import "math/rand"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// State represents the state of the replica.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func generateRand(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min+1) + min
}

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Written in 2A
	currentTerm  int
	votedFor     int
	lastLogIndex int
	lastLogTerm  int

	state State

	// Number of votes get.
	numVotes          int
	startFollowerTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// DPrintf("GetState starts")
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	// DPrintf("GetState finishes")
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

// =============================== RequestVote =================================
//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// For 2A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// For 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	var result string
	if args.Term < rf.currentTerm {
		// No vote
		reply.VoteGranted = false
		result = "NO_VOTE: small term"
	} else if args.Term == rf.currentTerm {
		canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		// TODO: check if the candidate log is newer than me.
		if canVote {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			result = "VOTE: same term, can vote"
		} else {
			reply.VoteGranted = false
			result = "NOVOTE: voted"
		}
	} else if args.Term > rf.currentTerm { // Update current term
		// In this case, we still allow to grant vote, regardless of whether
		// it has granted to itself. The rationale is that we are granting a vote
		// in a new term. This is to fix an interesting split vote as observed
		// in split_vote.example.
		DPrintf("%v(term %v) becomes follower, higher term %v from RequestVote",
			rf.me, rf.currentTerm, args.Term)
		rf.updateToHigherTermAndConvertToFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		result = "VOTE: higher term"
	}
	DPrintf("%v in RequestVote Handler, original term: %v, args term: %v, result: %v",
		rf.me, reply.Term, args.Term, result)
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Sends RequestVote and processes the response.
func (rf *Raft) requestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(server, args, reply) {
		// Check term to see if the request is initiated in the same term.
		rf.mu.Lock()
		// DPrintf("%v requestVote acquire lock", rf.me)
		defer func() {
			// DPrintf("%v requestVote release lock", rf.me)
			rf.mu.Unlock()
		}()
		DPrintf("%v(term %v) got vote resp(term: %v, granted %v), args(term: %v)",
			rf.me, rf.currentTerm, reply.Term, reply.VoteGranted, args.Term)
		if args.Term != rf.currentTerm {
			// We are in a different term now, the reply is useless
			return
		}

		if reply.Term > rf.currentTerm {
			rf.updateToHigherTermAndConvertToFollower(reply.Term)
			return
		}

		// We only care about the voting result when it's still candidate
		if rf.state == Candidate {
			if reply.VoteGranted {
				rf.numVotes++
				// DPrintf("%v adds vote to %v", rf.me, rf.numVotes)
				// Gets the majority of votes.
				if rf.numVotes > len(rf.peers)/2 {
					rf.state = Leader
				}
			}
		}
	}
}

func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Updates related states before initiating votes
	rf.currentTerm++
	DPrintf("%v(term %v): starts to request vote", rf.me, rf.currentTerm)
	rf.votedFor = -1
	rf.numVotes = 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// Vote for myself
			rf.numVotes++
			rf.votedFor = rf.me
		} else {
			// Send request vote RPC
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			go rf.requestVote(i, args)
		}
	}
	// DPrintf("%v finishes startRequestVote", rf.me)
}

// =============================== AppendEntries ==============================

type AppendEntriesArgs struct {
	// For 2A
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	// For 2A
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// For 2A
	// DPrintf("%v received AppendEntries, args.Term: %v, my term: %v",
	// 	rf.me, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v AppendEntries acquired lock, args.Term: %v, my term: %v",
	//	rf.me, args.Term, rf.currentTerm)
	if args.Term == rf.currentTerm {
		rf.state = Follower
		// Reset the time so that the follower won't time out and become candidate.
		rf.startFollowerTime = time.Now()
	} else if args.Term > rf.currentTerm {
		DPrintf("%v(term %v) becomes/stays follower, term %v from AppendEntries",
			rf.me, rf.currentTerm, args.Term)
		rf.updateToHigherTermAndConvertToFollower(args.Term)
	}

	// This is for leader to update itself if currentTerm is larger than Term.
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		// If leader sees a higer term, switch to follower
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			DPrintf("%v becomes follower, higher term from AppendEntries resp", rf.me)
			rf.updateToHigherTermAndConvertToFollower(reply.Term)
		}
	}
}

func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("leader %v(term %v): starts to send heartbeat", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			go rf.sendHeartBeat(i, args)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getStartFollowerTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.startFollowerTime
}

func (rf *Raft) setStartFollowerTime(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.startFollowerTime = t
}

// This is a general rule for all the servers.
//
// When seeing a higher term from the any RPC request
// or response, convert to Follower state, and updates to the higher term.
//
// REQUIRES: lock is already held.
func (rf *Raft) updateToHigherTermAndConvertToFollower(higherTerm int) {
	rf.currentTerm = higherTerm
	rf.votedFor = -1
	// We do not differentiate if the current state is Follower or not.
	rf.state = Follower
	rf.startFollowerTime = time.Now()
}

// ============================ State machines =================================

func (rf *Raft) FollowerState() {
	// Use 300 - 500 ms
	timeout := time.Duration(generateRand(100, 400)) * time.Millisecond
	rf.setStartFollowerTime(time.Now())
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	//
	// Check state in case it changes. We use shared state + lock to do
	// communication between goroutines.
	DPrintf("%v: went into follower state function with timeout %v", rf.me, timeout)
	for rf.getState() == Follower {
		// Time out
		if time.Now().Sub(rf.getStartFollowerTime()) > timeout {
			rf.mu.Lock()
			// It's very tricky that this will lead to deadlock, because
			// defer only executes when the function returns. while getState()
			// also needs the lock. It's not like C++ scoped_ptr.
			// defer rf.mu.Unlock()
			//
			// Instead explicitly unlock below.
			//
			// The earlier implementation checks the votedFor field and only becomes
			// candidate if the replica hasn't voted. This will prevent electing a
			// new leader in the following scenario.
			// - A becomes leader, B and C both vote for A.
			// - A becomes disconnected. B and C times out while waiting for
			//   heartbeat, but this condition prevents them from starting to request
			//   vote.
			// if rf.votedFor == -1 {
		  DPrintf("%v: in follower state, timed out, becomes candidate", rf.me)
			rf.state = Candidate
			rf.mu.Unlock()
			return
		}
		// DPrintf("%v: in follower state loop", rf.me)
		// TODO: change to a const variable
		// Sleep for 5 Milliseconds
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) CandidateState() {
	timeout := time.Duration(generateRand(400, 500)) * time.Millisecond
	// Set a time which is old enough to start a new election immediately.
	// The semantics here is that candidate is able to start new election now.
	// In follower, we will add some timeout as well.
	lastStartNewElectionTime := time.Now().Add(-2 * timeout)
	for rf.getState() == Candidate {
		// Check if we should start a new election
		if time.Now().Sub(lastStartNewElectionTime) > timeout {
			rf.startRequestVote()
			lastStartNewElectionTime = time.Now()
			timeout = time.Duration(generateRand(400, 500)) * time.Millisecond
		}
		// TODO: tune timeout
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) LeaderState() {
	// Tester limits you to 10 heartbeats per second.
	timeout := 120 * time.Millisecond
	// Set an old time to guarantee the first heartbeat
	lastStartHeartbeatTime := time.Now().Add(-2 * timeout)
	for rf.getState() == Leader {
		// Check if we need to repeat heartbeat
		now := time.Now()
		if now.Sub(lastStartHeartbeatTime) > timeout {
			lastStartHeartbeatTime = now
			rf.startHeartBeat()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) Run() {
	for {
		var state State
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			rf.FollowerState()
		case Candidate:
			rf.CandidateState()
		case Leader:
			rf.LeaderState()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// For 2A
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.state = Follower
	rf.startFollowerTime = time.Now() // Start with follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()
	return rf
}
