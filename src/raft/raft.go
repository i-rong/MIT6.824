package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.

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

const (
	ElectionTimeout  = time.Millisecond * 500
	HeartBeatTimeout = time.Millisecond * 120
)

type ServerType int

const (
	Follower ServerType = iota
	Candidate
	Leader
)

type VoteState int

const (
	Normal VoteState = iota // 投票过程正常
	Killed                  // Raft节点已经终止
	Expire                  // 投票(消息\竞选者过期)
	Voted                   // 本term内已经投过票了
)

type AppendEntriesState int

const (
	AppendNormal    AppendEntriesState = iota // 追加正常
	AppendOutofDate                           // 追加过时
	AppendKilled                              // Raft节点终止
	AppendRepeat                              //追加重复(2B)
	AppendCommitted                           // 追加的日志已经提交(2B)
	MissMatch                                 // 追加不匹配(2B)
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 需要持久化的变量
	currentTerm int // 记录当前的任期
	votedFor    int // 记录当前的任期将票投给了谁
	logs        []LogEntry

	commitIndex int // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）

	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	status        ServerType   // 该节点是什么角色（状态）
	electionTimer *time.Ticker // 每个节点中的计时器

	applyCh chan ApplyMsg // 日志都是存在这里client取（2B）
}

type LogEntry struct { // 如论文中所说 一个log entry是一个box 里面包含了这个entry的term和command
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.status == Leader
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
	Term         int // 候选人的term
	CandidateId  int // 正在请求投票的candidate
	LastLogIndex int // 候选人的最后一个log entry的index
	LastLogTerm  int // 候选人最后一个log entry的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       // server当前的term 便于让candidate更新自己
	VoteGranted bool      // true说明candidate收到了投票
	VoteState   VoteState // 投票状态
}

type AppendEntriesArgs struct {
	Term         int        // leader的term
	LeaderId     int        // 通过leaderId 可以让follower重定向到客户端
	PrevLogIndex int        // log中最后一个entry的index
	PrevLogTerm  int        // log中最后一个entry的term
	Entries      []LogEntry // 传入的entry 实现heartbeat时是空的
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	Term               int                // 当前server的term 便于让leader去更新自己
	Success            bool               // 如果那个被replicate entries的follower匹配上了prevLogIndex和prevLogTerm就返回true
	AppendEntriesState AppendEntriesState // 追加状态
	UpNextIndex        int                // 更新nextIndex[]
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
// 这里要注意sendRequestVote返回的是指调用api是否成功 峰会失败说明发送失败或者接收失败 它的成功与否和是否投票没有关系
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok { // 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return false
	}

	switch reply.VoteState {
	case Expire:
		{
			rf.status = Follower
			rf.electionTimer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNum <= len(rf.peers)/2 {
			*voteNum++
		}
		if *voteNum > len(rf.peers)/2 {
			*voteNum = 0
			if rf.status == Leader {
				return ok
			}
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs)
			}
			rf.electionTimer.Reset(HeartBeatTimeout)
		}
	case Killed:
		return false
	}
	return ok
}

// example RequestVote RPC handler.
// args是candidate rf是授予投票的服务器
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteState = Expire
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// 比candidate任期小的都已经把票还原了
	if rf.votedFor == -1 {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			reply.VoteState = Expire
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	} else { // 任期相同 有两种情况 一种是已经给这个人投过票了 还有一种是投给另一个人了
		reply.VoteState = Voted
		reply.VoteGranted = false
		if rf.votedFor != args.CandidateId {
			return
		} else {
			rf.status = Follower
		}
		rf.electionTimer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if the Raft instance is not the leader or has been killed, return gracefully
	if rf.status != Leader || rf.killed() {
		return index, term, false
	}
	DPrintf("Server[%v] (term[%v] status[%v]) receives a command[] and set it into log.", rf.me, rf.currentTerm, rf.status)
	// append the entry to the Raft's log
	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})

	return index, term, true
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
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		switch rf.status {
		case Follower:
			rf.status = Candidate
			fallthrough
		case Candidate:
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.electionTimer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)
			rf.startElection()
		case Leader:
			appendNums := 1
			rf.electionTimer.Reset(HeartBeatTimeout)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.logs) - 1,
					PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				args.Entries = rf.logs[rf.nextIndex[i]:]
				go rf.sendAppendEntries(i, &args, &reply, &appendNums)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	if rf.status != Candidate || rf.killed() {
		return
	}
	DPrintf("Server%v(term%v status%v) starts a election.", rf.me, rf.currentTerm, rf.status)
	voteNum := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply, &voteNum)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) {
	if rf.killed() {
		return
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.AppendEntriesState {
	case AppendKilled:
		return
	case AppendNormal:
		{
			if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
				*appendNum++
			}
			if rf.nextIndex[server] > len(rf.logs) {
				return
			}
			rf.nextIndex[server] += len(args.Entries)
			if *appendNum > len(rf.peers)/2 {
				*appendNum = 0
				if rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return
				}
			}
			for rf.lastApplied < len(rf.logs)-1 {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.lastApplied,
					Command:      rf.logs[rf.lastApplied].Command,
				}
				rf.applyCh <- applyMsg
				rf.commitIndex = rf.lastApplied
			}
			return
		}
	case MissMatch:
		if args.Term != rf.currentTerm {
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	case AppendOutofDate:
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.electionTimer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)
	case AppendCommitted:
		if args.Term != rf.currentTerm {
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	}
}

// args中的是leader rf是当前的server args向rf中添加entry
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Success = false
		reply.AppendEntriesState = AppendKilled
		reply.Term = -1
		return
	}

	if args.Term < rf.currentTerm {
		reply.AppendEntriesState = AppendOutofDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.AppendEntriesState = MissMatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	if rf.lastApplied > args.PrevLogIndex {
		reply.AppendEntriesState = AppendCommitted
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.electionTimer.Reset(time.Duration(150+rand.Intn(200)) * time.Millisecond)

	reply.AppendEntriesState = AppendNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}

	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied].Command,
		}
		rf.applyCh <- applyMsg
		rf.commitIndex = rf.lastApplied
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

	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{-1, -1}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.electionTimer = time.NewTicker(time.Duration(150+rand.Intn(200)) * time.Millisecond)
	go rf.ticker()

	return rf
}
