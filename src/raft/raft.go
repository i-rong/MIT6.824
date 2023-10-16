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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    ServerType          // 当前server的状态 三种 leader candidate follower

	currentTerm int        // server看到的最新的term
	votedFor    int        // server最新投票的候选人Id
	logs        []LogEntry // 存放这个server的log

	commitIndex int // 已知的最高提交的log entry的index
	lastApplied int // 最高应用到state machine的index

	nextIndex  []int // 下一个发送到该服务器的log entry的index
	matchIndex []int // 已知在服务器上复制的最高的log entry的index

	// 两个定时器 一个是election定时器 一个是heartbeat定时器
	// 超过election定时器 这个follower服务器就变成一个candidate
	// 每隔heartbeat时间 leader就要给所有的follower发一个空的appendentry 以此来告诉其他的follower leader还活着 不要开始新的选举
	// 讲道理 heartbeat应该比election时间要短
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// 为什么这里用指针 之后再看看
}

type LogEntry struct { // 如论文中所说 一个log entry是一个box 里面包含了这个entry的term和command
	index   int
	term    int
	command interface{}
}

type ServerType int

const (
	Follower ServerType = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	term         int // 候选人的term
	candidateId  int // 候选人的Id
	lastLogIndex int // 候选人的最后一个log entry的index
	lastLogTerm  int // 候选人最后一个log entry的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	term        int  // server当前的term 便于让candidate更新自己
	voteGranted bool // true说明candidate收到了投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果由于网络分区或者节点crash，导致candidate的term比请求投票的server端的term还要小 就直接返回
	if args.term < rf.currentTerm {
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
	}

	// 论文中说返回的term是server的currentTerm 便于让候选人去更新它自身
	reply.term = rf.currentTerm // reply的term设置为server的currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.candidateId) && args.lastLogTerm >= rf.currentTerm { // 按照论文里的 满足这两个条件就成功
		reply.voteGranted = true
		rf.votedFor = args.candidateId
		rf.currentTerm = args.term
		reply.term = rf.currentTerm
		return
	} else { // 失败
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
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
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return ok
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

// 如果是electionTimer时间到 那么就开启新一轮选举
// 如果是heartbeat时间到 并且是leader 那么就发起新的一轮心跳
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.status = Candidate               // 进入新的阶段 server变成candidate
			rf.currentTerm = rf.currentTerm + 1 // 新的一轮选举 term自增
			rf.StartElection()                  // 开始选举
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.status == Leader { // 如果是leader
				// rf.BroadCastHaertbeat(true)
				rf.heartbeatTimer.Reset((StableHeartbeatTimeout()))
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) StartElection() {
	request := genRequestVote()
	DPrintf("{Node: %v} starts election with Request %v", rf.me, request)
}

func (rf *Raft) BroadCastHaertbeat(is bool) {

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
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		status:         Follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()), // heartbeat应该是固定的时长  每隔一段时间leader就要发
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	lastEntry := rf.getLastEntry()    // 获取当前server的最后一个entry
	for i := 0; i < len(peers); i++ { // 循环遍历每一个服务器
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastEntry.index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) getLastEntry() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func StableHeartbeatTimeout() time.Duration {
	ret := 50 * time.Millisecond
	return ret
}

func RandomElectionTimeout() time.Duration {
	ms := 50 + (rand.Int63() % 300)
	ret := time.Duration(ms) * time.Millisecond
	return ret
}
