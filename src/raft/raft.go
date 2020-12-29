package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
//import "labgob"
import "math/rand"
import "time"

const (
	LEADER = 1
	FOLLOWER = 2
	CANDIDATE = 3
)

func max(x, y int) int {
    if x > y {
        return x
    }
    return y
}

func min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

type logentry struct{
	Data int
	Command interface{}
	Term int
	Index int
}

//
// as each Raft peer becomes aware that Successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	CommandValid bool

	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // Index into peers[]

	// Your Data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// persistent state on all servers
	currentTerm int
	votedFor int
	log []logentry
	
	// volatile state on all servers
	commitIndex int
	lastApplied int
	
	// volatile state on leaders
	nextIndex []int
	matchIndex []int
	
	state int
	votenum int
	applyCh chan ApplyMsg
	grantvoteCh chan bool
	winelectCh chan bool
	heartbeatCh chan bool
}

type AppendEntriesArgs struct {
	LeaderId int
	Term int
	PrevLogIndex int
	PrevLogTerm  int
	Entries []logentry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.log[len(rf.log)-1].Index + 1
		return
	}else if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	
	rf.heartbeatCh <- true
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
		reply.NextTryIndex = rf.log[len(rf.log)-1].Index + 1
		return
	}
	
	log_begin_id := rf.log[0].Index
	if args.PrevLogIndex >= log_begin_id && args.PrevLogTerm != rf.log[args.PrevLogIndex - log_begin_id].Term {
		Term := rf.log[args.PrevLogIndex - log_begin_id].Term
		for i := args.PrevLogIndex - 1; i >= log_begin_id; i-- {
			if rf.log[i - log_begin_id].Term != Term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= log_begin_id - 1{
		rf.log = rf.log[:args.PrevLogIndex - log_begin_id + 1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex < args.LeaderCommit {
			// update commitIndex and apply log
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			go rf.log_pad()
		}
	}
}


func (rf *Raft) AppendEntries_send(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != LEADER || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if reply.Success {
		if len(args.Entries) >= 1 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(rf.log[len(rf.log)-1].Index, reply.NextTryIndex)
	}
	log_begin_id := rf.log[0].Index
	for i := rf.log[len(rf.log)-1].Index; i > rf.commitIndex && rf.log[i - log_begin_id].Term == rf.currentTerm; i-- {
		count := 1
		for ii := range rf.peers {
			if ii != rf.me && rf.matchIndex[ii] >= i {
				count = count + 1
			}
		}
		
		if count > len(rf.peers) / 2 {
			rf.commitIndex = i
			go rf.log_pad()
			break
		}
	}
	
	return ok
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var Term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	Term = rf.currentTerm
	isleader = (rf.state == LEADER)
	defer rf.mu.Unlock()
	return Term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// Data := w.Bytes()
	// rf.persister.SaveRaftState(Data)
	Data := rf.rf_encode()
	rf.persister.SaveRaftState(Data)
}

func (rf *Raft) rf_encode() []byte{
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	Data := w.Bytes()
	return Data
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(Data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(Data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if Data != nil && len(Data) >= 1 {
		r := bytes.NewBuffer(Data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.log)
		d.Decode(&rf.votedFor)
	}
	
}

func (rf *Raft) getSnapshot(snapshot []byte) {
	if snapshot != nil && len(snapshot) >= 1 {
		var Last_id int 
		var Last_Term int
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)
		d.Decode(&Last_id)
		d.Decode(&Last_Term)
		rf.lastApplied = Last_id
		rf.commitIndex = Last_id
		rf.log_update(Last_id, Last_Term)
		msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
		rf.applyCh <- msg
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your Data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your Data here.
	Term int
	VoteGranted bool 
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
			 (args.LastLogTerm == rf.log[len(rf.log)-1].Term && 
			  args.LastLogIndex >= rf.log[len(rf.log)-1].Index)){
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.grantvoteCh <- true
		}else {
			reply.VoteGranted = false
		}
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
			 (args.LastLogTerm == rf.log[len(rf.log)-1].Term && 
			  args.LastLogIndex >= rf.log[len(rf.log)-1].Index)){
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.grantvoteCh <- true
		}else {
			reply.VoteGranted = false
		}
	}
	defer rf.mu.Unlock()
	defer rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) RequestVote_send(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok{
		if rf.state != CANDIDATE || rf.currentTerm != args.Term{
			return ok
		}
		if rf.currentTerm < reply.Term {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}
		if reply.VoteGranted {
			rf.votenum = rf.votenum + 1
			if rf.votenum > len(rf.peers) / 2{
				rf.state = LEADER
				rf.persist()
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				nextIndex := rf.log[len(rf.log)-1].Index + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.winelectCh <- true
			}
		}
	}
	
	return ok
}

func (rf *Raft) RequestVote_broadcast() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[len(rf.log)-1].Index
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.state == CANDIDATE {
			go rf.RequestVote_send(server, args, &RequestVoteReply{})
		}
	}
}


func (rf *Raft) log_pad() {
	rf.mu.Lock()
	log_begin_id := rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.Index = i
		msg.CommandValid = true
		msg.Command = rf.log[i - log_begin_id].Command
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
	defer rf.mu.Unlock()
}


type SnapshotArgs struct {
	Term int
	LeaderId int
	Last_id int
	Last_Term  int
	Data []byte
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) Snapshotting(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}else if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	
	rf.heartbeatCh <- true
	reply.Term = rf.currentTerm
	if args.Last_id > rf.commitIndex {
		rf.log_update(args.Last_id, args.Last_Term)
		rf.lastApplied = args.Last_id
		rf.commitIndex = args.Last_id
		
		rf.persister.SaveRaftState(rf.rf_encode()) //
		rf.persister.SaveSnapshot(args.Data)

		msg := ApplyMsg{Snapshot: args.Data, UseSnapshot: true}
		rf.applyCh <- msg
	}
}

func (rf *Raft) Snapshot_send(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Snapshotting", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != LEADER || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		// become follower and update current Term
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	rf.matchIndex[server] = args.Last_id
	rf.nextIndex[server] = args.Last_id + 1
	return ok
}


func (rf *Raft) Heartbeat_broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log_begin_id := rf.log[0].Index
	snapshot := rf.persister.ReadSnapshot()
	for server := range rf.peers {
		if server == rf.me || rf.state != LEADER {
			continue
		}
		if rf.nextIndex[server] <= log_begin_id{
			args := &SnapshotArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.Last_id = rf.log[0].Index
			args.Last_Term = rf.log[0].Term
			args.Data = snapshot
			go rf.Snapshot_send(server, args, &SnapshotReply{})
		}else{
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= log_begin_id {
				args.PrevLogTerm = rf.log[args.PrevLogIndex - log_begin_id].Term
			}
			if rf.nextIndex[server] <= rf.log[len(rf.log)-1].Index {
				args.Entries = rf.log[rf.nextIndex[server] - log_begin_id:]
			}
			args.LeaderCommit = rf.commitIndex
			go rf.AppendEntries_send(server, args, &AppendEntriesReply{})
		}
	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	Index := -1
	Term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	isLeader = (rf.state == LEADER)
	if isLeader {
		Term = rf.currentTerm
		Index = rf.log[len(rf.log)-1].Index + 1
		new_log := logentry{Index: Index, Term: Term, Command: command}
		rf.log = append(rf.log, new_log)
		rf.persist()
	}
	
	
	return Index, Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) state_change(Term int, state int) {
	if rf.state == state{
		return
	}
	rf.state = state
	if state == LEADER{
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i:=0; i<len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
		}
	}
	if state == FOLLOWER{
		rf.currentTerm = Term
		rf.votedFor = -1
	}
	if state == CANDIDATE{
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
	}
}

func (rf *Raft) log_update(Last_id int, Last_Term int) {
	new_log := make([]logentry, 0)
	new_log = append(new_log, logentry{Index: Last_id, Term: Last_Term})
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term != Last_Term || rf.log[i].Index != Last_id {
			continue
		}
		new_log = append(new_log, rf.log[i+1:]...)
		break
	}
	rf.log = new_log
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
func (rf *Raft) run() {
	for {
		if rf.state == LEADER{
			go rf.Heartbeat_broadcast()
			time.Sleep(60 * time.Millisecond)
		}else if rf.state == FOLLOWER{
			select {
			case <- rf.grantvoteCh:
			case <- rf.heartbeatCh:
			case <- time.After(time.Duration(rand.Intn(300)+200) * time.Millisecond):
				rf.state = CANDIDATE
				rf.persist()
			}
		}else if rf.state == CANDIDATE{
			rf.mu.Lock()
			rf.currentTerm = rf.currentTerm + 1
			rf.votedFor = rf.me
			rf.votenum = 1
			rf.persist()
			rf.mu.Unlock()
			go rf.RequestVote_broadcast()
			
			select {
			case <-rf.heartbeatCh:
				rf.state = FOLLOWER
			case <-rf.winelectCh:
			case <-time.After(time.Duration(rand.Intn(300)+200) * time.Millisecond):
			}
		}
		
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, logentry{Term: 0})
	rf.commitIndex = 0 //-1
	rf.lastApplied = 0 //-1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.grantvoteCh = make(chan bool, 100)
	rf.winelectCh = make(chan bool, 100)
	rf.heartbeatCh = make(chan bool, 100)
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	rf.getSnapshot(persister.ReadSnapshot())
	rf.persist()
	
	go rf.run()
	
	return rf
}
