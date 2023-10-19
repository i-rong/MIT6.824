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

const (
	ElectionTimeout   = time.Millisecond * 150
	HeartBeatInterval = time.Millisecond * 50
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	status      ServerType // 当前server的状态 三种 leader candidate follower
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
}

type LogEntry struct { // 如论文中所说 一个log entry是一个box 里面包含了这个entry的term和command
	Term    int
	Command interface{}
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
	Term        int  // server当前的term 便于让candidate更新自己
	VoteGranted bool // true说明candidate收到了投票
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
	Term    int  // 当前server的term 便于让leader去更新自己
	Success bool // 如果那个被replicate entries的follower匹配上了prevLogIndex和prevLogTerm就返回true
}

// example RequestVote RPC handler.
// args是candidate rf是授予投票的服务器
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(rf.getElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

// func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
// 	return true
// }

// func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
// 	ret := false
// 	if lastLogIndex == len(rf.logs)-1 && lastLogTerm == rf.logs[len(rf.logs)-1].Term {
// 		ret = true
// 	}
// 	return ret
// }

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
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.status = Candidate // 改变状态为candidate
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(rf.getElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.status == Leader { // 只有是leader才发送心跳包
				rf.BroadCastHeartBeat(true)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BroadCastHeartBeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			rf.heartbeatTimer.Reset(HeartBeatInterval)
			rf.electionTimer.Reset(rf.getElectionTimeout())
			continue
		}
		if isHeartBeat { // 如果是心跳包 发空的log entries就可以了
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(peer, &args, &reply)
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) { // 向server发送一个空的entry
	rf.mu.Lock()
	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}

	rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Unlock()
}

// 一个follower变成一个candidate发起选举流程
// rf是candidate
func (rf *Raft) StartElection() {
	if rf.status == Leader {
		rf.electionTimer.Reset(rf.getElectionTimeout())
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	DPrintf("{Server[%v] with term[%v]} starts a election with request %v.", rf.me, rf.currentTerm, args)
	// 为自己投票
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.electionTimer.Reset(rf.getElectionTimeout())
	// rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 请求每一个server为自己投票
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) { // 如果收到了来自peer的投票
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.status == Candidate {
					if reply.VoteGranted {
						DPrintf("{Server[%v] with term[%v]} receives vote from {Server[%v] with term[%v]}.", rf.me, rf.currentTerm, server, reply.Term)
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Server[%v] with term[%v] status[%v]} receives majority votes which is %v, and becomes a leader.", rf.me, rf.currentTerm, rf.status, grantedVotes)
							rf.status = Leader
							rf.BroadCastHeartBeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Server[%v] with term[%v]} has a term less than the term of server[%v] with term[%v], and it cannot be a leader.", rf.me, rf.currentTerm, peer, reply.Term)
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.votedFor = -1
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// args中的是leader rf是当前的server args向rf中添加entry
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("{Server[%v] with term[%v]}'s status is {status:%v, commitIndex:%v, lastApplied:%v} before processing AppendEntry.",
	// 	rf.me, rf.currentTerm, rf.status, rf.commitIndex, rf.lastApplied)

	// args中是leader rf中是follower
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.status = Follower                            // 既然收到了AppendEntries 那么一定是follower
	rf.electionTimer.Reset(rf.getElectionTimeout()) // 重置选举定时器
	DPrintf("{Server[%v] with term[%v]} receives entries from Server[%v].", rf.me, rf.currentTerm, args.LeaderId)

	reply.Term, reply.Success = rf.currentTerm, true
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatInterval)

	go rf.ticker()

	return rf
}

func (rf *Raft) getElectionTimeout() time.Duration {
	ms := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	// DPrintf("Election timeout %v.", ms)
	return ms
}
