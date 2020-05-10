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

//
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
	numVotes int

	// sends to it when it becomes leader (gets majority of votes)
	becomeLeaderCh chan bool
	// sends to it when it becomes follower or stay in follower
	becomeFollowerCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	DPrintf("GetState starts")
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	DPrintf("GetState finishes")
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

//
// example RequestVote RPC arguments structure.
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// For 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// No vote
		reply.VoteGranted = false
	} else {
		canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		// TODO: check if the candidate log is newer than me.
		if canVote {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}

		// Convert to follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
			DPrintf("server %v starts to send to channel in RequestVote", rf.me)
			rf.becomeFollowerCh <- true
			DPrintf("server %v finishes sending to channel in RequestVote", rf.me)
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Discover higer terms. Switch to follower state from leader or candidate.
	// For leader, shall we use > instead of >=

	// A universal rule for all RPC reqs and resp
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term >= rf.currentTerm && rf.state != Follower {
		DPrintf("%v starts to write to follower channel==========, pre state: %v", rf.me, rf.state)
		rf.state = Follower
		rf.becomeFollowerCh <- true
		DPrintf("%v finishes write to follwer channel ================", rf.me)
	} else if args.Term >= rf.currentTerm && rf.state == Follower {
		DPrintf("%v starts to write &&&&&&&&&&&&&&&", rf.me)
		rf.becomeFollowerCh <- true // stay in follower state
		DPrintf("%v finishes write &&&&&&&&&&&&&&&&", rf.me)
	}
	// The code below leads to a deadline
	/*
		if (args.Term >= rf.currentTerm && rf.state == Follower) {
	        DPrintf("%v starts to write &&&&&&&&&&&&&&&", rf.me)
			rf.becomeFollowerCh <- true	// stay in follower state
	        DPrintf("%v finishes write &&&&&&&&&&&&&&&&", rf.me)
		}*/
	// TODO what's the response
	// This is for leader to update itself if currentTerm is larger than Term.
	reply.Term = rf.currentTerm
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

// TODO: if grating
func (rf *Raft) FollowerState() {
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	for {
		DPrintf("%v: in follower state", rf.me)
		// TODO: tune
		// Use 400 - 500 ms
		timeout := generateRand(400, 500)
		timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
		select {
		case <-timer.C:
			// Time out
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.votedFor == -1 {
				rf.state = Candidate
				return
			}
			// TODO: add granting vote to candidate case
		case <-rf.becomeFollowerCh:
			// Continue
		}
	}
}

// Sends RequestVote and processes the response.
func (rf *Raft) requestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(server, args, reply) {
		// Check term to see if the request is initiated in the same term.
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term != rf.currentTerm {
			// We are in a different term now, the reply is useless
			return
		}

		// Switch to follower if seeing a higer term
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1 // reset votedFor in a new term
			rf.state = Follower
			DPrintf("%v writes to follower channel ^^^^^^^^^^^^^^^^^", rf.me)
			rf.becomeFollowerCh <- true
			DPrintf("%v finishes writing follower channel ^^^^^^^^^^^^^^^^^", rf.me)
			return
		}

		// We only care about the voting result when it's still candidate
		if rf.state == Candidate {
			if reply.VoteGranted {
				rf.numVotes++
				DPrintf("%v adds vote to %v", rf.me, rf.numVotes)
				// Gets the majority of votes.
				if rf.numVotes > len(rf.peers)/2 {
					rf.state = Leader
					DPrintf("%v writes to leader channel ^^^^^^^^^^^^^^^^^", rf.me)
					rf.becomeLeaderCh <- true
					DPrintf("%v finishes writing leader channel ^^^^^^^^^^^^^^^^^", rf.me)
				}
			}
		}
	}
}

func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v starts to request vote", rf.me)
	// Updates related states before initiating votes
	rf.currentTerm++
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
}

func (rf *Raft) CandidateState() {
	for {
		DPrintf("%v: in candidate state", rf.me)
		rf.startRequestVote()
		// TODO: tune timeout
		timeout := generateRand(400, 500)
		timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
		select {
		// Signal to become leader
		case <-rf.becomeLeaderCh:
			return
		// time out, start new round
		case <-timer.C:
		// It becomes follower (Logic in AppendEntries handler)
		case <-rf.becomeFollowerCh:
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		// If leader sees a higer term, switch to follower
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			// TODO: shall we update currentTerm to Term?
			// sendHeartBeat is sent from leader state. But when receiving the response,
			// it may not be in leader state any more.
			if rf.state != Follower {
				rf.state = Follower
				DPrintf("server %v starts to write to follower channel, ", rf.me)
				rf.becomeFollowerCh <- true
				DPrintf("server %v finishes write to follower channel, ", rf.me)
			}
			// A universal rule
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
	}
}

func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

func (rf *Raft) LeaderState() {
	for {
		DPrintf("%v: in leader state", rf.me)
		rf.startHeartBeat()
		// TODO: tune
		timeout := 150
		timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
		select {
		case <-timer.C:
			// Continue, will send next heartbeat
		case <-rf.becomeFollowerCh:
			return
		}
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
	rf.becomeLeaderCh = make(chan bool)
	rf.becomeFollowerCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()
	return rf
}
