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
	"fmt"
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

func (s ServerState) String() string {
	if s == Follower {
		return "F"
	} else if s == Candidate {
		return "C"
	} else if s == Leader {
		return "L"
	} else {
		panic("invalid state " + string(rune(s)))
	}
}

func (entry *LogEntry) String() string {
	return fmt.Sprintf("{%d t%d %v}", entry.Index, entry.Term, entry.Command)
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
	logs            []*LogEntry
	lastHeartbeat   time.Time     // 上一次收到leader发来的心跳包的时间
	electionTimeout time.Duration // 选举计时器

	commitIndex   int
	lastApplied   int
	applyCond     *sync.Cond
	nextIndex     []int
	matchIndex    []int
	replicateCond []*sync.Cond // 复制log的条件变量
}

func (rf *Raft) IsMajority(vote int) bool {
	return vote > rf.Majority()
}

func (rf *Raft) Majority() int {
	return len(rf.peers) / 2
}

func (rf *Raft) resetTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = nil
}

func (rf *Raft) GetLogAtIndex(index int) *LogEntry {
	if len(rf.logs) <= 1 { // 只有第一个占位用的entry直接返回空
		return nil
	}
	if index < 1 {
		panic("index < 1")
	}
	logicalLogSubscript := index - rf.logs[1].Index
	subscript := logicalLogSubscript + 1
	if len(rf.logs) > subscript {
		return rf.logs[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) LogTail() *LogEntry {
	return LogTail(rf.logs)
}

func LogTail(logs []*LogEntry) *LogEntry {
	return logs[len(logs)-1]
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
	now := time.Now()
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
	if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args) {
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartbeat = now
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) isAtLeastUpToDate(args *RequestVoteArgs) bool {
	ret := false
	if args.LastLogTerm == rf.LogTail().Term {
		ret = args.LastLogIndex >= rf.LogTail().Index
	} else {
		ret = args.LastLogTerm > rf.LogTail().Term
	}
	if !ret {
		rf.Debug(dVote, "hands down RequestVote from S%d %+v current log: %s", args.CandidateId, args, rf.FormatLog())
	}
	return ret
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
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
		rf.Debug(dLog, "receive AppendEntries from S%d args.term=%d %+v", args.LeaderId, args.Term, args)
		if args.Term > rf.currentTerm {
			rf.Debug(dLog, "received term %v > currentTerm %v from S%v, reset rf.currentTerm", args.Term, rf.currentTerm, args.LeaderId)
			rf.resetTerm(args.Term)
		}
		if args.PrevLogIndex > 0 {
			if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
				rf.Debug(dLog, "log consistency check failed. local log at prev {%d t%d}: %+v full log: %v", args.PrevLogIndex, args.PrevLogTerm, prev, rf.logs)
				if prev != nil { // conflict
					conflictIndex := prev.Index
					for i := prev.Index; i >= 1; i-- {
						if rf.logs[i].Term == prev.Term {
							conflictIndex = rf.logs[i].Index
						} else {
							break
						}
					}
					if conflictIndex > 1 {
						reply.ConflictIndex = rf.GetLogAtIndex(conflictIndex - 1).Index
						reply.ConflictTerm = rf.GetLogAtIndex(conflictIndex - 1).Term
					} else {
						reply.ConflictIndex = 0
						reply.ConflictTerm = 0 // TODO: may fail here
					}
				} else {
					reply.ConflictIndex = rf.LogTail().Index
					reply.ConflictTerm = rf.LogTail().Term
				}
				reply.Success = false
				return
			}
		}
		if len(args.Entries) > 0 {
			rf.Debug(dLog, "before merge: %s", rf.FormatLog())
			for _, entry := range args.Entries {
				local := rf.GetLogAtIndex(entry.Index)
				if local != nil {
					if local.Index != entry.Index {
						panic(rf.Sdebug(dFatal, "LMP violated: local.Index != entry.Index. headIndex=%d  local log at entry.Index: %+v", rf.logs[1].Index, local))
					}
					local.Term = entry.Term
					local.Command = entry.Command
				} else {
					// if rf.log[len(rf.log)-1].Index+1 != entry.Index {
					// 	panic(rf.Sdebug(dFatal, "rf.log[len(rf.log)-1].Index+1 != entry.Index. headIndex=%d", rf.log[1].Index))
					// }
					rf.logs = append(rf.logs, &LogEntry{
						Term:    entry.Term,
						Index:   entry.Index,
						Command: entry.Command,
					})
				}
			}
			argsTailIndex := LogTail(args.Entries).Index
			if rf.LogTail().Index > argsTailIndex {
				rf.logs = rf.logs[:argsTailIndex+1]
			}
			rf.logs[0].Index = rf.LogTail().Index
			rf.Debug(dLog, "after merge: %s", rf.FormatLog())
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.LogTail().Index)
			rf.applyCond.Broadcast()
		}
		rf.Debug(dLog, "finish process heartbeat: commitIndex=%d", rf.commitIndex)
		reply.Success = true
	} else if rf.state == Leader {
		if args.Term > rf.currentTerm {
			rf.Debug(dLog, "received term %v > currentTerm %v from S%v, back to Follower", args.Term, rf.currentTerm, args.LeaderId)
			rf.state = Follower
			rf.resetTerm(args.Term)
			reply.Success = true
		}
	}
}

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.logs[0].Index += 1 // Logs[0]用于记录当前log中最后一个entry的Index
	rf.logs = append(rf.logs, &LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.logs[0].Index,
		Command: command,
	})
	rf.matchIndex[rf.me] += 1
	rf.Debug(dClient, "client start replication with log %s %s", rf.FormatLog(), rf.FormatStateOnly())
	for peer := range rf.peers {
		rf.replicateCond[peer].Broadcast()
	}
	return rf.logs[0].Index, rf.currentTerm, rf.state == Leader
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
	for !rf.killed() {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if rf.state == Leader { // if Leader already
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastHeartbeat) > rf.electionTimeout { // 现在距离上一次收到heartbeat的时间超过了选举计时器
			rf.state = Candidate // 变成候选人
			rf.currentTerm += 1  // 任期+1
			rf.votedFor = &rf.me
			rf.Debug(dElection, "electionTimeout %dms elapsed, turning to Candidate", rf.electionTimeout/time.Millisecond)
			rf.electionTimeout = GetElectionTimeout() // 开始一场新选举重置选举计时器

			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.LogTail().Index,
				LastLogTerm:  rf.LogTail().Term,
			}
			rf.mu.Unlock()
			vote := 1
			for peer := range rf.peers { // 向所有其他的peer请求投票
				if peer == rf.me {
					continue
				}
				go func(server int) {
					var reply RequestVoteReply
					ok := rf.sendRequestVote(server, args, &reply)
					if !ok { // 这里的ok表示的是发送是不是成功 不代表有没有收到投票
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.Debug(dElection, "S%v RequestVoteReply %+v rf.term=%v args.Term=%v", server, reply, rf.currentTerm, args.Term)
					if rf.state != Candidate || rf.currentTerm != args.Term { // drop
						return
					}
					if reply.Term > rf.currentTerm {
						rf.Debug(dElection, "return to Follower due to reply.Term > rf.currentTerm")
						rf.state = Follower
						rf.resetTerm(reply.Term)
						return
					}
					if reply.VoteGranted {
						rf.Debug(dElection, "<- S%v vote received", server)
						vote += 1
						if rf.IsMajority(vote) {
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.LogTail().Index + 1
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = rf.LogTail().Index
							rf.Debug(dLeader, "majority vote (%d/%d) received, turning Leader %s", vote, len(rf.peers), rf.FormatState())
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
	rf.Debug(dHeartbeat, "heartbeat start")
	term := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == leaderId {
			continue
		}
		go func(server int) {
			rf.Sync(server, &AppendEntriesArgs{
				Term:     term,
				LeaderId: leaderId,
				Entries:  nil,
			})
		}(peer)
	}
	rf.mu.Lock()
}

func (rf *Raft) DoHeartbeat() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval)
		if !rf.needHeartbeat() {
			continue
		}
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go rf.Replicate(peer)
		}
	}
}

func (rf *Raft) needHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) DoReplicate(server int) {
	rf.replicateCond[server].L.Lock()
	defer rf.replicateCond[server].L.Unlock()
	for {
		for !rf.needReplicate(server) {
			rf.replicateCond[server].Wait()
			if rf.killed() {
				return
			}
		}
		rf.Replicate(server)
	}
}

func (rf *Raft) needReplicate(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextIndex := rf.nextIndex[server]
	rf.Debug(dTrace, "needReplicate: nextIndex=%v log tail %+v", rf.nextIndex, rf.LogTail())
	return rf.state == Leader && server != rf.me && rf.GetLogAtIndex(nextIndex) != nil && rf.LogTail().Index >= nextIndex
}

func (rf *Raft) Replicate(server int) { // 由leader向server复制
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	var entries []*LogEntry
	nextIndex := rf.nextIndex[server]
	for j := nextIndex; j < len(rf.logs); j++ {
		entry := *rf.logs[j]
		entries = append(entries, &entry)
	}
	prevLogIndex := 0
	prevLogTerm := 0
	if nextIndex > 1 {
		prevLogIndex = rf.logs[nextIndex-1].Index
		prevLogTerm = rf.logs[nextIndex-1].Term
		rf.Debug(dWarn, "replicate S%d nextIndex=%v matchIndex=%v prevLog: %v", server, rf.nextIndex, rf.matchIndex, rf.GetLogAtIndex(prevLogIndex))
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()
	rf.Sync(server, args) // 同步日志
}

func (rf *Raft) Sync(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dHeartbeat, "S%d AppendEntriesReply %+v rf.term=%d args.Term=%d", server, reply, rf.currentTerm, args.Term)
	if rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.Debug(dHeartbeat, "return to Follower due to reply.Term > rf.term")
		rf.state = Follower
		rf.resetTerm(reply.Term)
	} else {
		if reply.Success {
			if len(args.Entries) == 0 {
				return
			}
			logTailIndex := LogTail(args.Entries).Index
			rf.matchIndex[server] = logTailIndex
			rf.nextIndex[server] = logTailIndex + 1
			rf.Debug(dHeartbeat, "S%d logTailIndex=%d commitIndex=%d matchIndex=%v nextIndex=%v", server, logTailIndex, rf.commitIndex, rf.matchIndex, rf.nextIndex)
			preCommitIndex := rf.commitIndex
			for i := rf.commitIndex + 1; i <= logTailIndex; i++ {
				count := 0
				for peer := range rf.peers {
					if rf.matchIndex[peer] >= i {
						count += 1
					}
				}
				if rf.IsMajority(count) && rf.logs[i].Term == rf.currentTerm {
					preCommitIndex = i
				}
			}
			rf.commitIndex = preCommitIndex
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		} else {
			rf.matchIndex[server] = 0
			rf.nextIndex[server] = reply.ConflictIndex + 1
		}
	}
}

func (rf *Raft) DoApply(applyCh chan ApplyMsg) {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		for !rf.needApply() {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
		rf.mu.Lock()
		rf.lastApplied += 1
		nextCommitLog := rf.logs[rf.lastApplied]
		rf.Debug(dCommit, "apply rf[%d]=%+v current log: %s", rf.lastApplied, nextCommitLog, rf.FormatLog())
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      nextCommitLog.Command,
			CommandIndex: nextCommitLog.Index,
		}
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dTrace, "needApply: commitIndex=%d lastApplied=%d", rf.commitIndex, rf.lastApplied)
	return rf.state != Candidate && rf.commitIndex > rf.lastApplied
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

	rf.applyCond = sync.NewCond(&sync.Mutex{})
	rf.electionTimeout = GetElectionTimeout()
	rf.logs = append(rf.logs, &LogEntry{
		Term:    0,
		Index:   0,
		Command: nil,
	})
	go rf.DoElection()
	go rf.DoHeartbeat()
	go rf.DoApply(applyCh)
	for range rf.peers {
		rf.replicateCond = append(rf.replicateCond, sync.NewCond(&sync.Mutex{}))
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	for peer := range rf.peers {
		go rf.DoReplicate(peer)
	}

	if len(rf.logs) != 1 {
		panic("len(rf.log != 1)")
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
