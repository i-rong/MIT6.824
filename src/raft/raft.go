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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
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

type ServerState int8

const (
	Follower ServerState = iota
	Candidate
	Leader
)

const (
	ElectionTimeoutMax = int64(500 * time.Millisecond)
	ElectionTimeoutMin = int64(300 * time.Millisecond)
	HeartbeatInterval  = 200 * time.Millisecond
)

func GetElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeoutMin + rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin))
}

func (state ServerState) String() string {
	if state == Follower {
		return "F"
	} else if state == Candidate {
		return "C"
	} else if state == Leader {
		return "L"
	} else {
		panic("invalid state " + string(rune(state)))
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

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
	state           ServerState
	currentTerm     int
	votedFor        *int
	logs            []LogEntry
	lastHeartbeat   time.Time     // 上一次收到leader发来的心跳包的时间
	electionTimeout time.Duration // 选举计时器
}

func (rf *Raft) resetTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.votedFor == nil {
		rf.Debug(dVote, "S%v RequestVote %+v votedFor=<nil>", args.CandidateId, args)
	} else {
		rf.Debug(dVote, "S%v RequestVote %+v votedFor=S%v", args.CandidateId, args, *rf.votedFor)
	}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.Debug(dVote, "received term %v > currentTerm %v, back to Follower", args.Term, rf.currentTerm)
		rf.state = Follower
		rf.resetTerm(args.Term)
	}
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.Debug(dLog, "received term %v  < currentTerm %v from S%v, reject AppendEntries", args.Term, rf.currentTerm, args.LeaderId)
		reply.Success = false
		return
	}
	rf.lastHeartbeat = now

	if rf.state == Candidate {
		if args.Term >= rf.currentTerm {
			rf.Debug(dLog, "received term %v >= currentTerm %v from S%v, leader is legitimate", args.Term, rf.currentTerm, args.LeaderId)
			rf.state = Follower
			rf.resetTerm(args.Term)
			reply.Success = true
		}
	} else if rf.state == Follower {
		if args.Term > rf.currentTerm {
			rf.Debug(dLog, "received term %v > currentTerm %v from S%v, reset rf.currentTerm", args.Term, rf.currentTerm, args.LeaderId)
			rf.resetTerm(args.Term)
			reply.Success = true
		} else { // args.Term = rf.currentTerm
			rf.Debug(dLog, "receives AppendEntries from S%v(term%v)", args.LeaderId, args.Term)
			// handle log entries
		}
	} else if rf.state == Leader {
		if args.Term > rf.currentTerm {
			rf.Debug(dLog, "received term %v > currentTerm %v from S%v, back to Follower", args.Term, rf.currentTerm, args.LeaderId)
			rf.state = Follower
			rf.resetTerm(args.Term)
			reply.Success = true
		} else { // args.Term = rf.currentTerm
			panic(rf.Sdebug(dFatal, "Split brain: receive AppendEntries from S%v at the same term", args.LeaderId))
		}
	}
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

func (rf *Raft) DoElection() {
	for {
		time.Sleep(rf.electionTimeout)
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state == Leader { // if Leader already
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastHeartbeat) > rf.electionTimeout { // 现在距离上一次收到heartbeat的时间超过了选举计时器
			rf.currentTerm += 1
			rf.state = Candidate
			rf.votedFor = &rf.me
			rf.Debug(dElection, "electionTimeout %vms elapsed, turning to Candidate", rf.electionTimeout/time.Millisecond)
			rf.electionTimeout = GetElectionTimeout() // 开始一场新选举重置选举计时器

			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.mu.Unlock()
			vote := 1
			for peer := range rf.peers { // 向所有其他的peer请求投票
				if peer == rf.me {
					continue
				}
				go func(server int) {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(server, args, reply)
					if !ok { // 这里的ok表示的是发送是不是成功 不代表有没有收到投票
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.Debug(dElection, "S%v RequestVoteReply %+v rf.term=%v args.term=%v", server, reply, rf.currentTerm, args.Term)
					if rf.state != Candidate || rf.currentTerm != args.Term { // drop
						return
					}
					if reply.Term > rf.currentTerm {
						rf.Debug(dElection, "return to Follower due to reply.Term > rf.currentTerm")
						rf.state = Follower
						rf.resetTerm(args.Term)
						return
					}
					if reply.VoteGranted {
						rf.Debug(dElection, "<- S%v vote received", server)
						vote += 1
						if vote > len(rf.peers)/2 {
							rf.Debug(dLeader, "majority vote (%d/%d) received, turning to Leader", vote, len(rf.peers))
							rf.state = Leader
							rf.BroadcastHeartbeat()
						}
					}
				}(peer)
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) BroadcastHeartbeat() {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != args.Term { // term confusion
				return
			}
			if reply.Term > rf.currentTerm {
				rf.Debug(dHeartbeat, "return to Follower due to reply.term ? rf.term")
				rf.state = Follower
				rf.resetTerm(reply.Term)
			}
		}(peer)
	}
	rf.mu.Lock()
}

func (rf *Raft) DoHeartbeat() {
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state == Leader {
			rf.BroadcastHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatInterval)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimeout = GetElectionTimeout()
	go rf.DoElection()
	go rf.DoHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
