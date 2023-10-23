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

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var bt int64 = 0

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
	ElectionTimeout   = time.Millisecond * 500
	HeartBeatInterval = time.Millisecond * 150
)

const (
	CHECKAPPENDPERIOD float64 = 10
	CHECKCOMMITPERIOD float64 = 10
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
	cond        *sync.Cond
	applyCh     chan ApplyMsg

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
	Entries []LogEntry
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
// 这里要注意sendRequestVote返回的是指调用api是否成功 峰会失败说明发送失败或者接收失败 它的成功与否和是否投票没有关系
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 返回值表示是否收到投票
func (rf *Raft) callRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	DPrintf("Candidate[%v] (term[%v] status[%v]) ask Server[%v] to vote for it.", rf.me, rf.currentTerm, rf.status, server)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 比较请求投票和响应投票时的term 如果不相同 直接将这个reply丢弃
	if term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	return reply.VoteGranted
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
// 如果是heartbeat时间到 并且是leader 那么就发起新的一轮心跳
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.StartElection()
		case <-rf.heartbeatTimer.C:
			if rf.status == Leader { // 只有是leader才发送心跳包
				go rf.BroadCastHeartBeat()
			}
		}
	}
}

func (rf *Raft) BroadCastHeartBeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			rf.heartbeatTimer.Reset(HeartBeatInterval)
			rf.electionTimer.Reset(rf.getElectionTimeout())
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.logs) - 1,
			PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peer, &args, &reply)
	}
}

func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) (bool, []LogEntry) {
	DPrintf("Server[%v] (term[%v] status[%v]) send entries[] to Server[%v].", rf.me, rf.currentTerm, rf.status, server)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		temp := make([]LogEntry, 0)
		return false, temp
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm {
		temp := make([]LogEntry, 0)
		return false, temp
	}

	if reply.Term > rf.currentTerm {
		DPrintf("Server[%v]'s term is less than Server[%v]'s term, Server[%v] change to Follower.", rf.me, server, rf.me)
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	return reply.Success, reply.Entries
}

// 检查当前server是否有新放进来还没有发送给follower的log entry
func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.status != Leader { // 如果挂了 或者不是leader 优雅地返回
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		lastLogIndex := len(rf.logs) - 1
		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1 // peer的最后一个log entry
		prevLogTerm := rf.logs[prevLogIndex].Term
		leaderCommit := rf.commitIndex
		entries := rf.logs[nextIndex:] // 从nextIndex往后都是需要附加过来的 前提要保证nextIndex <= lastLogIndex
		rf.mu.Unlock()
		if lastLogIndex >= nextIndex { // 说明有需要附加到peer的entry
			success, _ := rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, entries, leaderCommit)
			rf.mu.Lock()
			if term != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			if success {
				rf.nextIndex[server] = nextIndex + len(entries)
				rf.matchIndex[server] = prevLogIndex + len(entries)
				DPrintf("Server[%v] (term[%v] status[%v]) send real appendEntries[] to Server[%v] successfully.", rf.me, rf.currentTerm, rf.status, server)
				DPrintf("Leader[%v] entries[]   VS   Server[%v] entries[].", rf.me, server)
			} else {
				rf.nextIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
		}
		time.Sleep((time.Microsecond * time.Duration(CHECKAPPENDPERIOD)))
	}
}

// 分配len(rf.peers)个go routine去检查是否需要附加entry
func (rf *Raft) allocateAppendCheckers() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.appendChecker(peer) // 每一个server检查自己是否需要附加entry
	}
}

// leader检查是否commit成功
func (rf *Raft) commitChecker() {
	for {
		consensus := 1 // 已经提交到server的个数 自己算一个
		rf.mu.Lock()
		if rf.commitIndex < len(rf.logs)-1 {
			nextCommitIndex := rf.commitIndex + 1 // 下一个要提交的entry的index
			for peer := range rf.peers {
				if peer == rf.me {
					continue
				}
				if rf.matchIndex[peer] >= nextCommitIndex {
					consensus++
				}
			}
			if consensus > len(rf.peers)/2 && rf.logs[nextCommitIndex].Term == rf.currentTerm {
				DPrintf("Server[%v] (term[%v] status[%v]) commit log entry[] successfully.", rf.me, rf.currentTerm, rf.status)
				rf.commitIndex = nextCommitIndex
				rf.cond.Broadcast()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Microsecond * time.Duration(CHECKCOMMITPERIOD))
	}
}

// 一个follower变成一个candidate发起选举流程
// rf是candidate
func (rf *Raft) StartElection() {
	DPrintf("Server[%v] (term[%v] status[%v]) starts a election, and becomes a Candidate.", rf.me, rf.currentTerm, rf.status)
	// 为自己投票
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.status = Candidate
	rf.electionTimer.Reset(rf.getElectionTimeout())
	term := rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	rf.mu.Unlock()
	// rf.persist()
	grantedVotes := 1
	electionFinished := false // 确保只进行一次选举
	var voteMutex sync.Mutex
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 请求每一个server为自己投票
		go func(server int) {
			voteGranted := rf.callRequestVote(server, term, lastLogIndex, lastLogTerm)
			voteMutex.Lock()
			if voteGranted && !electionFinished {
				grantedVotes++
				if grantedVotes > len(rf.peers)/2 {
					DPrintf("Server[%v] (term[%v] status[%v]) receives majority of votes and becomes leader.", rf.me, rf.currentTerm, rf.status)
					electionFinished = true // 选举成功
					rf.mu.Lock()
					rf.status = Leader
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					rf.BroadCastHeartBeat()
					go rf.allocateAppendCheckers()
					go rf.commitChecker()
				}
			}
			voteMutex.Unlock()
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

	// args中是leader rf中是follower
	if args.Term < rf.currentTerm { // 如果Leader的term < 当前server的term
		DPrintf("Server[%v] (term[%v] status[%v])'s term > Leader[%v]'s term[%v], update leader.", rf.me, rf.currentTerm, rf.status, args.LeaderId, args.Term)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm { // 如果leader的term > server
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(rf.getElectionTimeout()) // 重置选举定时器

	// reply false if log doesn't contain an entry at preLogIndex whose term matches preLogTerm
	// remember to handle the case where prevLogIndex points beyond the end of your log
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// delete conflicting entries and append new entries
	// same index but different terms delete the existing entry and all that follow it
	i := 0
	j := args.PrevLogIndex + 1 // j等于leader的log中已提交的长度也等于new entries的第一个entry的下标

	hasConfilict := false
	for i = 0; i < len(args.Entries); i++ { // 遍历即将附加进来的entries
		if j >= len(rf.logs) {
			break
		}
		if rf.logs[j].Term == args.Entries[i].Term { // 如果相等 继续往后比较
			j++
		} else { // 如果不相等 把后面的都drop掉
			rf.logs = append(rf.logs[:j], args.Entries[i:]...)
			DPrintf("Server[%v] not equel!!! update to rf.logs[%v].", rf.me, rf.logs)
			i = len(args.Entries)
			j = len(rf.logs) - 1
			hasConfilict = true
			break
		}
	}

	if i < len(args.Entries) { // 如果后面还有剩的没复制上来
		rf.logs = append(rf.logs, args.Entries[i:]...)
		j = len(rf.logs) - 1 // 获取最后一个log entry的下标
	} else if !hasConfilict {
		j-- // 如果没有过冲突
	}

	reply.Success = true
	reply.Entries = rf.logs

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j)))
		DPrintf("Server[%v] (term[%v] status[%v])'s commitIndex is %v.", rf.me, rf.currentTerm, rf.status, rf.commitIndex)
		if rf.commitIndex > oldCommitIndex { // 说明有更新 通知leader
			rf.cond.Broadcast() // 唤醒所有被条件变量阻塞的协程
		}
	}
}

func (rf *Raft) applyCommited() {
	for {
		// 这里要注意 不是只有leader才可以apply
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex { // 如果应用到状态机上的entry的下标不小于服务器上最高被提交的entry的话，
			// 说明所有已经提交的都已经应用到状态机上面了 那么就将条件变量阻塞住，等待新的被提交的entry
			rf.cond.Wait()
		}
		rf.lastApplied++ // 得到下一个应该应用的下标
		DPrintf("Server[%v] (term[%v] status[%v]) try to apply log entry[] to the state machine.", rf.me, rf.currentTerm, rf.status)
		cmtidx := rf.lastApplied
		command := rf.logs[cmtidx].Command // 得到下一个应该应用的指令
		rf.mu.Unlock()
		// 接下来应用这个指令
		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: cmtidx,
		}
		// 应用指令，这里有可能被管道阻塞
		rf.applyCh <- msg
		DPrintf("Server[%v] (term[%v] status[%v]) apply log entry[] to the state machine successfully. Now the logs are[].", rf.me, rf.currentTerm, rf.status)
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
	rf.cond = sync.NewCond(&rf.mu) // 条件变量和锁关联
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatInterval)

	go rf.ticker()

	// start a background go routine that periodically apply the committed log entry
	go rf.applyCommited()
	return rf
}

func (rf *Raft) getElectionTimeout() time.Duration {
	ms := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	return ms
}
