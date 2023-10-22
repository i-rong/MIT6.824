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

	state       ServerState // Server的状态 三种之一 Follower Candidate Leader
	currentTerm int         // 该Server所处的term
	voteFor     int         // 该Server为谁投票
	logEntries  []LogEntry  // log entries
	commitIndex int         // 最后一个已提交的日志下标 从0开始
	lastApplied int         // 最后一个应用到状态机的日志下标 从0开始

	nextIndex  []int // 每个Follower的下一条entry的下标，初始化为leader最后一条entry的下标+1
	matchIndex []int // 保存每个Follower最后一条与当前leader一致的entry，初始化为0. 也相当于follower最后一条commit的日志

	appendCh chan *AppendEntriesArgs // 用于Follower判断在ElectionTimeout期间有没有收到过AppendEntries
	voteCh   chan *RequestVoteArgs   // 永久接收候选人发来的请求投票信息
	applyCh  chan ApplyMsg           // 每commit一个entry 就执行这个entry的命令 在实现中 执行这个命令相当于往这个管道中写数据

	electionTimeout *time.Timer // 选举时间定时器 一旦到时 Follower就变成Candidate参加选举
	heartBeat       int         // 心跳时间定时器

	log bool
}

type ServerState int

type LogEntry struct {
	Term    int // 该entry所属的Term
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int        // Leader的term
	LeaderId     int        // Leader的Id
	PrevLogIndex int        // 新加入的entries之前entry的下标
	PrevLogTerm  int        // 新加入的entries之前的entry的term
	Entries      []LogEntry // 要附加进去的entries
	LeaderCommit int        // Leader的CommitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前Server的term 用于更新Leader自己
	Success bool // 返回是否附加成功
}

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
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
	Term         int // 候选人的term
	CandidateId  int // 候选人的Id
	LastLogIndex int // 最后一个entry的下标
	LastLogTerm  int // 最后一个entry的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 被请求投票的Server的term 用于Candidate更新自己
	VoteGranted bool // 表示是否投票
}

// 站在follower的角度完成下面这个rpc调用
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// follower收到leader的信息
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// args.Term >= rf.currentTerm
	if args.PrevLogIndex >= len(rf.logEntries) || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	i := 0
	j := args.PrevLogIndex + 1
	hasConflict := false
	for i < len(args.Entries) {
		if j >= len(rf.logEntries) {
			break
		}
		if rf.logEntries[j].Term == args.Entries[i].Term {
			i++
			j++
		} else {
			rf.logEntries = append(rf.logEntries[:j], args.Entries[i:]...)
			i = len(args.Entries)
			j = len(rf.logEntries)
			hasConflict = true
			break
		}
	}

	if i < len(args.Entries) {
		rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
		j = len(rf.logEntries)
	} else if !hasConflict {
		j--
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j)))
	}

	go func() {
		rf.appendCh <- args
	}()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		if args.LastLogIndex >= len(rf.logEntries)-1 && args.LastLogTerm >= rf.logEntries[len(rf.logEntries)-1].Term {
			rf.voteFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			go func() {
				rf.voteCh <- args
			}()
		}
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

func (rf *Raft) changeState(state ServerState) {
	switch state {
	case Follower:
		DPrintf("me:%2d term:%3d | state:%2d change role to Follower!\n", rf.me, rf.currentTerm, rf.state)
		rf.state = Follower
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		DPrintf("me:%2d term:%3d | state:%2d change role to Candidate!\n", rf.me, rf.currentTerm, rf.state)
		rf.state = Candidate
	case Leader:
		DPrintf("me:%2d term:%3d | state:%2d change role to Leader!\n", rf.me, rf.currentTerm, rf.state)
		rf.state = Leader
	}
}

// Follower需要做的任务：
// 1.接收leader的appendEntries，重置选举定时器
// 2.投票给Candidate，重置选举定时器
// 3.选举定时器超时，转变为Candidate
func (rf *Raft) Follower() {
	for {
		rf.resetElectionTimeout()
		select {
		case args := <-rf.appendCh:
			// 接收到entries信息
			DPrintf("me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.LeaderId)
		case args := <-rf.voteCh:
			DPrintf("me:%2d term:%3d | role:%2d vote to candidate %3d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId)
		case <-rf.electionTimeout.C: // 选举定时器到时
			DPrintf("me:%2d term:%3d | follower timeout!\n", rf.me, rf.currentTerm)
			rf.changeState(Candidate)
			return
		}
	}
}

// Leader需要做的任务:
// 1.每秒钟发送10次心跳包给所有的节点
// 2.接受到更高的term leader的心跳信息 转为follower
// 3.append返回的term高于自己的term 转为follower
func (rf *Raft) Leader() {
	rf.mu.Lock()
	lastLogIndex := len(rf.logEntries) - 1
	term := rf.currentTerm
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.nextIndex[peer] = lastLogIndex + 1
		rf.matchIndex[peer] = 0
	}

	for {
		commitFlag := false // 超半数commit只通知一次管道
		commitNum := 1
		commitLock := sync.Mutex{}
		// 广播消息
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			rf.mu.Lock()
			// newEntries := make([]LogEntry, 1)
			// prevLogIndex := rf.nextIndex[peer] - 1
			// prevLogTerm := rf.logEntries[prevLogIndex].Term
			appendArgs := AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			// 请求其他server的投票
			go func(server int) {
				reply := AppendEntriesReply{}
				DPrintf("me:%2d term:%3d | leader send message to %3d\n", rf.me, term, server)
				if ok := rf.peers[server].Call("Raft.AppendEntries", &appendArgs, &reply); ok {
					rf.mu.Lock()
					if rf.currentTerm != term {
						// 如果server当前的term不等于发送信息的term
						// 表明这是一条过时的信息 直接丢弃即可
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					if reply.Success {
						commitLock.Lock()
						commitNum++
						if !commitFlag && commitNum > len(rf.peers)/2 {
							commitFlag = true
							commitLock.Unlock()
						} else {
							commitLock.Unlock()
						}
					} else { // append失败
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							DPrintf("me: %2d term %2d | is not the highest term, need to change to Follower.", rf.me, rf.currentTerm)
							rf.changeState(Follower)
						} else {
							// prevLogIndex或者prevLogTerm不匹配
							rf.nextIndex[server] = rf.nextIndex[server] - 1
							rf.mu.Unlock()
						}
					}
				}
			}(peer)
		}
		select {
		case <-rf.appendCh:
			rf.changeState(Follower)
			return
		case <-rf.voteCh:
			rf.changeState(Follower)
			return
		case <-time.After(time.Duration(rf.heartBeat) * time.Millisecond):

		}

	}
}

// Candidate需要做的任务:
// 1.发起选举 获得超过一半的投票变为leader
// 2.投票给更高term的candidate
// 3.接收leader发来的entries 转变为follower
// 4.定时器超时 重新投票
func (rf *Raft) Candidate() {
	rf.mu.Lock()
	// Candidate的term已经在changeState里面+1了
	// 发送给每个server的请求是一样的
	rf.voteFor = rf.me // 先给自己投票
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term
	requestArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	voteCnt := 1      // 记录有多少个server为我投票了
	beSelect := false // 确保只收到一次成为Leader
	voteOk := make(chan bool)
	voteLock := sync.Mutex{} // 互斥锁 防止资源竞争
	// 开始请求别人的投票 重置选举定时器
	rf.resetElectionTimeout()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		DPrintf("me: %2d term %2d | ask server %2d to vote", rf.me, rf.currentTerm, peer)
		go func(server int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(server, &requestArgs, &reply); ok {
				// ok仅仅代表收到了回复
				// ok == false代表本次发送的信息丢失 或者是回复的信息丢失
				if reply.VoteGranted {
					voteLock.Lock()
					voteCnt += 1
					if !beSelect && voteCnt > len(rf.peers)/2 {
						DPrintf("me: %2d term %2d receives majority of votes.", rf.me, rf.currentTerm)
						beSelect = true
						voteLock.Unlock()
						voteOk <- true
					} else {
						voteLock.Unlock()
					}
				}
			}

		}(peer)
	}

	select {
	case args := <-rf.appendCh:
		// 收到心跳
		DPrintf("me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.LeaderId)
		rf.changeState(Follower)
		return
	case args := <-rf.voteCh:
		// 投票给某人
		DPrintf("me:%2d term:%3d | state:%2d vote to candidate %3d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId)
		rf.changeState(Follower)
		return
	case <-rf.electionTimeout.C:
		// 重新开始选举
		DPrintf("me:%2d term:%3d | candidate timeout!\n", rf.me, rf.currentTerm)
		rf.changeState(Candidate)
		return
	case <-voteOk:
		rf.changeState(Leader)
		return
	}
}

func (rf *Raft) ticker() {
	for {
		switch rf.state {
		case Follower:
			rf.Follower()
		case Candidate:
			rf.Candidate()
		case Leader:
			rf.Leader()
		}
	}
}

func (rf *Raft) getRand() int64 {
	ret := 150 + rand.Int63()%150
	return ret
}

// 重置选举定时器
func (rf *Raft) resetElectionTimeout() {
	if rf.electionTimeout == nil {
		rf.electionTimeout = time.NewTimer(time.Duration(rf.getRand()) * time.Millisecond)
	} else {
		rf.electionTimeout.Reset(time.Duration(rf.getRand()) * time.Millisecond)
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

	rf.state = Follower // 初始化为Follower
	rf.currentTerm = 0
	rf.voteFor = -1 // 没有给任何人投票
	rf.logEntries = make([]LogEntry, 1)
	rf.logEntries[0] = LogEntry{0, 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.appendCh = make(chan *AppendEntriesArgs)
	rf.voteCh = make(chan *RequestVoteArgs)
	rf.applyCh = applyCh

	rf.heartBeat = 100 // 每秒发送10次心跳包

	rf.log = true

	DPrintf("Create a new server:[%3d]! term:[%3d]\n", rf.me, rf.currentTerm)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
