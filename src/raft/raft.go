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

import "fmt"
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

// Use upper case field names which is required by RPC package.
type logEntry struct {
	Term 		int
	Command interface{}
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
	state State

	// Number of votes get.
	numVotes          int
	startFollowerTime time.Time

	// Written in 2B
	logs []*logEntry
	commitIndex int
	lastApplied int

	// Volatile state on leaders, reinitialized after election.
	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg
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

	// For 2B
	rf.logs = make([]*logEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
  rf.nextIndex = make([]int, len(peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	for i, _:= range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()
	return rf
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

// Returns true if the candidate is at least as update to date as the receiver.
//
// REQUIRES: locks are already held.
func (rf *Raft) upToDate(args *RequestVoteArgs) bool {
	// When receiving the args, the candiadte's log must have at least one
	// element.
	numLogs := len(rf.logs)
	if numLogs == 0 {
		return true
	}
	myLastLogTerm := rf.logs[numLogs - 1].Term
	if args.LastLogTerm > myLastLogTerm {
		return true
	}
	if args.LastLogTerm < myLastLogTerm {
		return false
	}
	// The same last log term, compare log length.
	return args.LastLogIndex >= numLogs
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// For 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	upToDate := rf.upToDate(args)

	var result string
	if args.Term < rf.currentTerm {
		// No vote
		reply.VoteGranted = false
		result = "NO_VOTE: small term"
	} else if args.Term == rf.currentTerm {
		canVote := upToDate &&
							 (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
		if canVote {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			result = "VOTE: same term, can vote"
		} else {
			reply.VoteGranted = false
			result = fmt.Sprintf("NO_VOTE: already voted to %v", rf.votedFor)
		}
	} else if args.Term > rf.currentTerm { // Update current term
		// In this case, we still allow to grant vote, regardless of whether
		// it has granted to itself. The rationale is that we are granting a vote
		// in a new term. This is to fix an interesting split vote as observed
		// in split_vote.example.
		if upToDate {
			DPrintf("%v(term %v) becomes follower, higher term %v from RequestVote",
							rf.me, rf.currentTerm, args.Term)
			rf.updateToHigherTermAndConvertToFollower(args.Term)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			result = "VOTE: higher term"
		} else {
			result = "NOVOTE: stale logs"
		}
	}
	DPrintf("%v in RequestVote Handler, my original term: %v, args term: %v, result: %v",
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

// Called when getting the majority of votes as a candidate.
//
// REQUIRES: locks are already held.
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + 1
		rf.matchIndex[i] = 0
	}
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
				// Gets the majority of votes and becomes leader.
				if rf.numVotes > len(rf.peers)/2 {
					rf.becomeLeader()
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
			numLogs := len(rf.logs)
			if numLogs > 0 {
				args.LastLogIndex = numLogs
				args.LastLogTerm = rf.logs[numLogs - 1].Term
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

	// For 2B
	PrevLogIndex int
	PrevLogTerm int
	Entries []*logEntry
  LeaderCommit int
}

func (args AppendEntriesArgs) DebugString() string {
  entries := "["
	for _, entry := range args.Entries {
			entries += fmt.Sprintf("%v, ", entry)
	}
	entries += "]"
	return fmt.Sprintf("AppendEntriesArgs(Term: %v, LeaderId: %v, " +
		"PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v, LeaderCommit: %v",
		 args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, entries,
		 args.LeaderCommit)
}

type AppendEntriesReply struct {
	// For 2A
	Term int
	// For 2B
	Success bool
}

func (rf *Raft) LogsDebugString() string {
	s := "["
	for _, log := range rf.logs {
		s += fmt.Sprintf("(%v), ", *log)
	}
	s += "]"
	return s
}

// TODO(libochen): I should probably unify the client code of heartbeat and
// AppendEntries, to retry properly to allow logs appended to the follower via
// heartbeat mechanism.
//
// Note AppendEntries can also be used as a heartbeat msg, where args.Entries
// is empty. Make sure the logics here apply to both heartbeat and normal
// AppendEntries.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("%v received AppendEntries, args.Term: %v, my term: %v",
	// 	rf.me, args.Term, rf.currentTerm)
	rf.mu.Lock()

	defer func() {
		// Some common logic that must be executed.
		//
		// Convert to follower if seeing a higher term.
		if args.Term > rf.currentTerm {
			DPrintf("%v(term %v) becomes/stays follower, term %v from AppendEntries",
							rf.me, rf.currentTerm, args.Term)
			rf.updateToHigherTermAndConvertToFollower(args.Term)
	  }
		// This is for leader to update itself if currentTerm is larger than Term.
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Check if log contains an entry at prevLogIndex whose term matches
	// prevLogTerm
	if (args.PrevLogIndex > 0) {
		// Either it doesn't contain an entry or the term doesn't match.
		if (len(rf.logs) < args.PrevLogIndex ||
				rf.logs[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
	}

	// Start to append logs.
	//
	// Note We should not truncate the logs if all the logs match,
	// in case this is an outdated AppendEntries RPC from the leader.
	reply.Success = true
  i := args.PrevLogIndex
	for _, entry := range args.Entries {
		// It doesn't have entry in the index.
		if (len(rf.logs) < i + 1) {
			rf.logs = append(rf.logs, entry)
			DPrintf("%v(term %v) starts to append log %v, current logs: %v",
							rf.me ,rf.currentTerm, entry, rf.LogsDebugString())
		} else if (rf.logs[i].Term != entry.Term) {
			rf.logs[i] = entry
			// Delete all the entries after i.
			rf.logs = rf.logs[0: i]
		}
		i++
	}

	// As a follower, update commitIndex and apply if possible.
	if (args.LeaderCommit > rf.commitIndex) {
		indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
		if (args.LeaderCommit < indexOfLastNewEntry) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = indexOfLastNewEntry
		}
	}
	rf.maybeApply()

  // This is to update state to Follower if seeing a term not less than
	// currentTerm.
	if args.Term == rf.currentTerm {
		rf.state = Follower
		// Reset the time so that the follower won't time out and become candidate.
		rf.startFollowerTime = time.Now()
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	{
		rf.mu.Lock()
		DPrintf("%v(term %v) sends Heartbeat(args: %v) to server %v.",
						rf.me, rf.currentTerm, args.DebugString(), server)
		rf.mu.Unlock()
	}
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
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
 			// Notes here we should include the leader's commitIndex so that follower
			// have a chance to apply their logs.
			//
			// When the leader starts a new aggrement for a specific log, the follower
			// can not commit the log in that round. Instead, they knew the log is
			// committed in later rounds from leader's commmitIndex.
			//
			// We should also make sure the other fields, e.g. PrevLogTerm are set
			// properly, since AppendEntries RPC handler is shared between normal
			// AppendEntries and the heartbeat use case. The main difference here
			// is that Entries is empty.
			//
			// TODO: introduce a common function to construct the args.
			args := &AppendEntriesArgs {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			if (args.PrevLogIndex >= 1) {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex - 1].Term
			}
			go rf.sendHeartBeat(i, args)
		}
	}
}

// REQUIRES: locks must be held already.
func (rf *Raft) matchInMajority(index int) bool {
	n := 0
	numServers := len(rf.peers)
	for i := 0; i < numServers; i++ {
		if (rf.matchIndex[i] >= index) {
			n++
		}
	}
	DPrintf("%v(term %v) in matchInMajority: matches %v for index %v", rf.me,
					rf.currentTerm, n, index)
	return n >= (numServers / 2 + 1)
}

// Compare commitIndex and lastApplied, and apply if possible.
//
// REQUIRES: locks must be held already.
func (rf *Raft) maybeApply() {
	// Check if we can apply new logs.
	for rf.lastApplied < rf.commitIndex {
		DPrintf("%v(term %v) starts to apply logs[%v]", rf.me, rf.currentTerm,
						rf.lastApplied)
		rf.applyCh <- ApplyMsg{
										CommandValid: true,
										Command: rf.logs[rf.lastApplied].Command,
										CommandIndex: rf.lastApplied + 1,
									}
		rf.lastApplied++
	}
}

// Check if we can increase commitIndex, and apply new logs.
//
// REQUIRES: locks must be held already.
func (rf *Raft) maybeCommit() {
	DPrintf("%v(term %v) in maybeCommit(), rf.commitIndex: %v", rf.me,
	        rf.currentTerm, rf.commitIndex)
	// commitIndex increases monotonically, which avoids duplicate apply.
	//
	// Note: logIndex starts from 1 in Raft protocol
	for logIndex := rf.commitIndex + 1; logIndex <= len(rf.logs); logIndex++ {
		// The second condition is due to the fact that a leader can not determine
		// if a log entry has committed in a previous term.
		if rf.matchInMajority(logIndex) &&
		   rf.logs[logIndex-1].Term == rf.currentTerm {
			rf.commitIndex = logIndex
		}
	}
	rf.maybeApply()
}

func (rf *Raft) startAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	for true {
		{
			rf.mu.Lock()
			DPrintf("%v(term %v) sends AppendEntries(args: %v) to server %v to replicate log.",
							rf.me, rf.currentTerm, args.DebugString(), server)
			rf.mu.Unlock()
			if !rf.sendAppendEntries(server, args, reply) {
				break
			}
		}
		rf.mu.Lock()
		// This is probably a necessary check.
		if (rf.state != Leader || rf.currentTerm != args.Term) {
			rf.mu.Unlock()
			return
		}
		if (reply.Success) {
			DPrintf("%v(term %v), starts to handle sucessful AppendEntries resp",
							rf.me, rf.currentTerm)
			// Use this instead of len(rf.logs) in case the logs changed since the
			// RPC was sent.
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			// We need to check this, in case we decrease the matchIndex if it's
			// already increased by later RPCs due to RPC delay etc.
			if (newMatchIndex > rf.matchIndex[server]) {
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				rf.maybeCommit()
			}
			rf.mu.Unlock()
			return
		} else {
			// Retry RPC
			//
			// When manipulating the indexes, be careful that rf.nextIndex[server]
			// may change since the RPC was sent.
			//
			// rf.nextIndex[server]--  // do not update this.
			args.PrevLogIndex--
			if (args.PrevLogIndex < 0) {
				DPrintf("Impossible scenario")
				rf.mu.Unlock()
				return
			}
			if (args.PrevLogIndex > 0) {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex - 1].Term
			}
			// args.Entries starts from args.PrevLogIndex until the end.
			args.Entries = []*logEntry{} // empty the Entries field.
			for i := args.PrevLogIndex + 1; i < len(rf.logs); i++ {
				args.Entries = append(args.Entries, rf.logs[i-1])
			}
			rf.mu.Unlock()
		}
	}

	// TODO: universal rule to update terms and convert to follower.
	// Suspect it should not affect correctness.
}

// REQUIRES: locks are already held.
func (rf *Raft) startAgreement() {
	// Find replicas and send AppendEntries RPC to them.
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// If last log index >= nextIndex for a follower, send AppendEntries RPC.
		if len(rf.logs) >= rf.nextIndex[i] {
				args := &AppendEntriesArgs {
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
				}
				if (args.PrevLogIndex >= 1) {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex - 1].Term
				}
				args.Entries = []*logEntry{}
				for j := rf.nextIndex[i] - 1 ; j < len(rf.logs); j++ {
					args.Entries = append(args.Entries, rf.logs[j])
				}
				go rf.startAppendEntries(i, args)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// For 2B.
	// TODO: not sure at this moment. Suspect it should not block and return
	// immediately. This should be well understood!
	if (rf.state != Leader) {
		return 0, 0, false
	}
	rf.logs = append(rf.logs, &logEntry{Term: rf.currentTerm, Command: command})
  rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me]++
	rf.startAgreement()
	return len(rf.logs), rf.currentTerm, true
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

func (rf *Raft) followerState() {
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

func (rf *Raft) candidateState() {
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

func (rf *Raft) leaderState() {
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
			rf.followerState()
		case Candidate:
			rf.candidateState()
		case Leader:
			rf.leaderState()
		}
	}
}
