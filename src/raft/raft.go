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
import "labrpc"
import "bytes"
import "encoding/gob"
import "labgob"
import "math/rand"
import "time"

const (
	LEADER = 1
	FOLLOWER = 2
	CANDIDATE = 3
)


type logentry struct{
	data int
	Command interface{}
	term int
	index int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
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
	me        int // index into peers[]

	// Your data here.
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

func (rf *Raft) AppendEntries(
	leaderId int,
	term int,
	prevLogIndex int,
	entries []logentry,
	leaderCommit int
)(int, bool){
	var current_term int
	var success bool
	
	return current_term, success

}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	defer rf.mu.Unlock()
	return term, isleader
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
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.persist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) rf_encode() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil && len(data) >= 1 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.log)
		d.Decode(&rf.votedFor)
	}
	
}

func (rf *Raft) recoverFromSnapshot(snapshot []byte) {
	if snapshot != nil && len(snapshot) >= 1 {
		var last_id int 
		var last_term int
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&last_id)
		d.Decode(&last_term)
		rf.lastApplied = last_id
		rf.commitIndex = last_id
		rf.log_update(last_id, last_term)
		msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
		rf.applyCh <- msg
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term int
	voteGranted bool 
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	if args.term < rf.currentTerm {
		reply.term = rf.currentTerm
		reply.voteGranted = false
	}else if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.state = FOLLOWER
		rf.votedFor = -1
		
		reply.term = rf.currentTerm
		reply.voteGranted = false
	}else{
		reply.term = rf.currentTerm
		reply.voteGranted = false
		
		if (rf.votedFor == -1 || rf.votedFor == args.candidateId) && (args.lastLogTerm > rf.log[len(rf.log)-1].term || (args.lastLogTerm == rf.log[len(rf.log)-1].term && args.lastLogTerm >= rf.log[len(rf.log)-1].index)){
			rf.votedFor = args.candidateId
			reply.grantvoteCh = true
			rf.chanGrantVote <- true
		}
	}
	defer rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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


	return index, term, isLeader
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


func (rf *Raft) state_change(term int, state int) {
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
		rf.currentTerm = term
		rf.votedFor = -1
		rf.heartbeat.stop()
		rf.election.start()
	}
	if state == CANDIDATE{
		rf.currentTerm += 1
		rf.votedFor = rf.me
		
	}
}

func (rf *Raft) log_update(last_id int, last_term int) {
	new_log := make([]logentry, 0)
	new_log = append(new_log, logentry{index: last_id, term: last_term})
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].term != last_term || rf.log[i].index != last_id {
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, logentry{term: 0})
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	rf.mu.Lock()
	
	
	rf.persist()
	
	defer rf.mu.Unlock()
	return rf
}
