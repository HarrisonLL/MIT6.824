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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// enums for state of each server
type State string

const (
	Leader    State = "Leader"
	Follower  State = "Follower"
	Candidate State = "Candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int //  The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
	matchIndex []int

	// others
	state         State
	voteCnt       int
	grantVoteCh   chan bool
	heartbeatCh   chan bool
	winElectionCh chan bool
	stepDownCh    chan bool
	applyCh       chan ApplyMsg

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (rf *Raft) toSliceIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil ||
		e.Encode(rf.lastIncludedTerm) != nil {
		panic("Failed to encode raft persistent state")
	}
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.getRaftState())
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	sliceIdx := rf.toSliceIndex(index)
	rf.lastIncludedTerm = rf.log[sliceIdx].Term
	rf.log = rf.log[sliceIdx:]
	rf.lastIncludedIndex = index
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil ||
		d.Decode(&rf.lastIncludedTerm) != nil {
		panic("Failed to decode raft persistent state")
	}
}

// send value to unbuffered channel without blocking
// this is non-blocking send, where message is either sent or dropped
// if just do ch <- value, and no goroutine was waiting on the channel,
// the sender would block forever.
func (rf *Raft) sendToChannel(channel chan bool, value bool) {
	select {
	case channel <- value:
	default:
	}
}

// All Server rules: If RPC request or response contains termT > currentTerm: set current Term = T, convert to follower
func (rf *Raft) stepDownToFollower(term int) {
	needToSend := rf.state != Follower
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	if needToSend {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}

// All Server rules: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
// The service expects your implementation to send an ApplyMsg for each newly committed log entry to the applyCh channel argument to Make().
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	start := max(rf.lastApplied+1, rf.lastIncludedIndex+1)
	for i := start; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.toSliceIndex(i)].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

// === request Vote ===
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(requestVoteArgs *RequestVoteArgs) bool {
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	if lastTerm == requestVoteArgs.LastLogTerm {
		return requestVoteArgs.LastLogIndex >= lastIndex
	}
	return requestVoteArgs.LastLogTerm > lastTerm

}

// example RequestVote RPC handler.
// Receiverimplementation:
// 1. Reply false if term < currentTerm(§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote(§5.2,§5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.sendToChannel(rf.grantVoteCh, true)
	}
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

	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If Call reply late (when current server is not candidate / args or reply has past term),
	// Then return false
	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return false
	}
	if rf.currentTerm < reply.Term {
		rf.stepDownToFollower(reply.Term)
		rf.persist()
		return false
	}

	if reply.VoteGranted {
		rf.voteCnt += 1
		if rf.voteCnt == len(rf.peers)/2+1 { // vote just exceed majority
			rf.sendToChannel(rf.winElectionCh, true)
		}
	}
	return ok
}

// broadcast vote request to each peer
func (rf *Raft) broadcastVoteRequest() {
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &requestVoteArgs, &RequestVoteReply{})
		}
	}
}

// === Snapshot ===
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)
	// Stale snapshot request
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	if args.LastIncludedIndex < rf.lastLogIndex() {
		rf.log = rf.log[rf.toSliceIndex(args.LastIncludedIndex):]
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      args.Data,
			CommandIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		rf.persist()
		return
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
}

// === Append Entry ===
type AppendEntriesRPCArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRPCReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}
	// edge cases where none follower recieve appendEntries
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}
	rf.sendToChannel(rf.heartbeatCh, true)
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reply.Success = false
	reply.Term = rf.currentTerm
	// 2: opt: Case 1: follower log too short
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	// entries already covered by snapshot, treat as match
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	// opt: Case 2: term mismatch
	prevLogTerm := rf.log[rf.toSliceIndex(args.PrevLogIndex)].Term
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		for i := args.PrevLogIndex; i >= rf.lastIncludedIndex && rf.log[rf.toSliceIndex(i)].Term == reply.ConflictTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	// 3 - 4
	i := args.PrevLogIndex + 1
	j := 0
	for i < rf.lastLogIndex()+1 && j < len(args.Entries) {
		if rf.log[rf.toSliceIndex(i)].Term != args.Entries[j].Term {
			break
		}
		i += 1
		j += 1
	}
	rf.log = rf.log[:rf.toSliceIndex(i)]
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true

	// 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		go rf.applyLogs()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRPCArgs, reply *AppendEntriesRPCReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return false
	}

	if rf.currentTerm < reply.Term {
		rf.stepDownToFollower(reply.Term)
		return false
	}

	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		if reply.ConflictTerm == -1 {
			// opt: case 1: log too shot
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// opt: case 2: search for conflict term
			newNextIdx := rf.lastLogIndex()
			for ; newNextIdx >= rf.lastIncludedIndex; newNextIdx-- {
				if rf.log[rf.toSliceIndex(newNextIdx)].Term == reply.ConflictTerm {
					break
				}
			}
			if newNextIdx < rf.lastIncludedIndex {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = newNextIdx
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	}

	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.lastLogIndex(); n >= rf.commitIndex && n >= rf.lastIncludedIndex; n-- {
		cnt := 1
		if rf.log[rf.toSliceIndex(n)].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}

	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				go rf.sendInstallSnapshot(i)
			} else {
				prevLogIndex := rf.nextIndex[i] - 1
				entries := make([]LogEntry, len(rf.log[rf.toSliceIndex(rf.nextIndex[i]):]))
				copy(entries, rf.log[rf.toSliceIndex(rf.nextIndex[i]):])
				args := AppendEntriesRPCArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[rf.toSliceIndex(prevLogIndex)].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesRPCReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
	}
}

// randomized election timeout 360 - 600 ms
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// convert raft state to leader
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.broadcastAppendEntries()
}

// convert raft state to candidate
func (rf *Raft) convertToCandidate(fromState State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.voteCnt = 1
	rf.persist()

	rf.broadcastVoteRequest()
}

func (rf *Raft) resetChannels() {
	rf.winElectionCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()
	return rf.lastLogIndex(), rf.currentTerm, true

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

func (rf *Raft) runServer() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(120 * time.Millisecond): // send heartbeat
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTimeout() * time.Millisecond): // timeout heartbeat/election
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectionCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond): // timeout heartbeat/election
				rf.convertToCandidate(Candidate)
			}
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.winElectionCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.log = append(rf.log, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// Your initialization code here (2A, 2B, 2C).
	go rf.runServer()

	return rf
}
