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

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const TIMEOUTHIGH float64 = 1000 // 等待成为candidate的时间是500-1000ms
const TIMEOUTLOW float64 = 500
const HEARTBEAT float64 = 150

const CHECKPERIOD float64 = 300 // check timeout per 300ms

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
	// electionTimer  *time.Timer
	// heartbeatTimer *time.Timer
	// 为什么这里用指针 之后再看看
	timestamp time.Time // 上一次收到leader的heartbeat的时间戳
}

type LogEntry struct { // 如论文中所说 一个log entry是一个box 里面包含了这个entry的term和command
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
	term         int // 候选人的term
	candidateId  int // 正在请求投票的candidate
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

type AppendEntriesArgs struct {
	term         int        // leader的term
	leaderId     int        // 通过leaderId 可以让follower重定向到客户端
	prevLogIndex int        // log中最后一个entry的index
	prevLogTerm  int        // log中最后一个entry的term
	entries      []LogEntry // 传入的entry 实现heartbeat时是空的
	leaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	term    int  // 当前server的term 便于让leader去更新自己
	success bool // 如果那个被replicate entries的follower匹配上了prevLogIndex和prevLogTerm就返回true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{term %v}: Server[%v] receives RequestVote from Server[%v].", rf.currentTerm, rf.me, args.candidateId)
	// args是candidate rf是授予投票的server

	if args.term < rf.currentTerm { // 这个候选人的任期比我还要小 不给它投
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
	}

	if args.term > rf.currentTerm { // 这个候选人的任期比我高 我肯定不能成为leader了 我自动变成follower
		rf.status = Follower
		rf.currentTerm = args.term
		rf.votedFor = -1
	}

	reply.term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.candidateId {
		lastLogTerm := rf.logs[len(rf.logs)-1].term
		if args.lastLogTerm > lastLogTerm || (args.lastLogTerm == lastLogTerm && args.lastLogIndex >= len(rf.logs)-1) {
			// 只有再投票给另一个server的时候才更新时间戳
			rf.timestamp = time.Now()
			rf.votedFor = args.candidateId
			reply.voteGranted = true
			DPrintf("{term %v}: Server[%v] votes for Server[%v]", rf.currentTerm, rf.me, rf.votedFor)
			return
		}
	}
	reply.voteGranted = false
	return
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
	r := rand.New(rand.NewSource(int64(rf.me)))

	for {
		if rf.killed() {
			break
		}
		timeout := int(r.Float64()*(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW) // 等待变成candidate的时间
		rf.mu.Lock()
		if time.Since(rf.timestamp) > time.Duration(timeout)*time.Millisecond && rf.status != Leader {
			go rf.StartElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(CHECKPERIOD))
	}
}

// 一个follower变成一个candidate发起选举流程
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.currentTerm += 1       // currentTerm自增
	rf.status = Candidate     // 成为candidiate
	rf.votedFor = rf.me       // 先为自己投一票
	rf.timestamp = time.Now() // timestamp记的是上一次投票的时间
	term := rf.currentTerm

	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].term
	rf.mu.Unlock()

	DPrintf("{term : %v}: Server[%v] with state[%v] starts an election", term, rf.me, rf.status)
	grantedVotes := 1 // count the num of grant to vote
	electionFinished := false
	var voteMutex sync.Mutex
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			voteGranted := rf.callRequestVote(peer, term, lastLogIndex, lastLogTerm)
			voteMutex.Lock()
			if voteGranted && !electionFinished { // 如果当前这个peer给我投票了 并且这个选举阶段还没结束
				grantedVotes += 1                   // 给我投票了 记一个
				if grantedVotes > len(rf.peers)/2 { // 超过一半的人为我投票了 那就结束
					electionFinished = true
					rf.mu.Lock()
					rf.status = Leader // 成为leader
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					go rf.leaderHeartBeat()
				}
			}
			voteMutex.Unlock()
		}(peer)
	}
}

// leader每隔一段时间发送一下heartbeat 告诉其他server我还或者 你们别急着选新的
func (rf *Raft) leaderHeartBeat() {
	DPrintf("{term : %v}: Server[%v] with state[%v] becomes a leader.", rf.currentTerm, rf.me, rf.status)
	for { // go routine 一直循环
		rf.mu.Lock()
		if rf.killed() || rf.status != Leader { // 如果噶了或者已经不是一个leader了
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		prevLogIndex := len(rf.logs) - 1
		prevLogTerm := rf.logs[prevLogIndex].term
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()
		for peer := range rf.peers { // 向每一个peer发heartbeat信息
			if peer == rf.me {
				continue
			}
			go func(server int) {
				rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, make([]LogEntry, 0), leaderCommit)
			}(peer)
		}
		time.Sleep(time.Millisecond * time.Duration(HEARTBEAT)) // 每隔heartbeat ms发送一次
	}
}

// rf是leader server是其他服务器
func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) bool {
	DPrintf("{term : %v}: Server[%d] with state[%d] sends appendentries RPC to server[%v].", rf.currentTerm, rf.me, rf.status, server)
	args := AppendEntriesArgs{
		term:         term,
		leaderId:     rf.me,
		prevLogIndex: prevLogIndex,
		prevLogTerm:  prevLogTerm,
		entries:      entries,
		leaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return false
	}

	if reply.term > rf.currentTerm {
		rf.currentTerm = reply.term
		rf.status = Follower
		rf.votedFor = -1
	}
	return reply.success
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
	DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d]", rf.currentTerm, rf.me, rf.status, args.leaderId)

	// reply false immediately if sender's term < currentTerm
	if args.term < rf.currentTerm {
		reply.success = false
		return
	}

	// other server has higher term !
	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.status = Follower
		rf.votedFor = -1
	}

	reply.term = rf.currentTerm

	// to reach this line, the sender must have equal or higher term than me(very likely to be the current leader), reset timer
	rf.timestamp = time.Now()

	// reply false if log doesn't contain an entry at preLogIndex whose term matches preLogTerm
	// remember to handle the case where prevLogIndex points beyond the end of your log
	if args.prevLogIndex >= len(rf.logs) || rf.logs[args.prevLogIndex].term != args.prevLogTerm {
		reply.success = false
		return
	}

	// delete conflicting entries and append new entries
	i := 0
	j := args.prevLogIndex + 1
	DPrintf("%d", j)
	for i = 0; i < len(args.entries); i++ {
		if j >= len(rf.logs) {
			break
		}
		if rf.logs[j].term == args.entries[i].term {
			j++
		} else {
			rf.logs = append(rf.logs[:j], args.entries[i:]...)
			i = len(args.entries)
			j = len(rf.logs) - 1
			break
		}
	}
	if i < len(args.entries) {
		rf.logs = append(rf.logs, args.entries[i:]...)
		j = len(rf.logs) - 1
	} else {
		j--
	}
	reply.success = true

	if args.leaderCommit > rf.commitIndex {
		// set commitIndex = min(leaderCommit, index of last **new** entry)
		// oriCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.leaderCommit), float64(j)))
		DPrintf("[term %d]:Raft [%d] [state %d] commitIndex is %d", rf.currentTerm, rf.me, rf.status, rf.commitIndex)
		// if rf.commitIndex > oriCommitIndex {
		// 	// wake up sleeping applyCommit Go routine
		// 	rf.cond.Broadcast()
		// }
	}
}

func (rf *Raft) callRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	requestArgs := RequestVoteArgs{
		term:         term,
		candidateId:  rf.me,
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &requestArgs, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return false
	}

	if reply.term > rf.currentTerm {
		rf.status = Follower // 这人的任期比我高 它更适合当leader 我就不当了
		rf.currentTerm = reply.term
		rf.votedFor = -1
	}
	return reply.voteGranted
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
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		status:      Follower,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		votedFor:    -1,
		logs:        make([]LogEntry, 0),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}

	rf.logs = append(rf.logs, LogEntry{term: 0})
	rf.timestamp = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
