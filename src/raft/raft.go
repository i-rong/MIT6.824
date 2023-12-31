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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	ElectionTimeoutMax = int64(600 * time.Millisecond)
	ElectionTimeoutMin = int64(400 * time.Millisecond)
	HeartbeatInterval  = 100 * time.Millisecond
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
	votedFor        int
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
	// return vote > rf.Majority()
	return vote >= rf.Majority()
}

func (rf *Raft) Majority() int {
	// return len(rf.peers) / 2
	return len(rf.peers)/2 + 1
}

func (rf *Raft) resetTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) GetLogAtIndex(index int) *LogEntry {
	if index < rf.logs[0].Index {
		return nil
	}
	subscript := rf.LogIndexToSubscript(index)
	if len(rf.logs) > subscript {
		return rf.logs[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) LogIndexToSubscript(index int) int { // 由entry中的Index转为rf.logs中的下标
	return index - rf.logs[0].Index
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.Debug(dPersist, "persist: %s", rf.FormatStateOnly())
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*LogEntry
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if e := d.Decode(&currentTerm); e != nil {
		panic(e)
	}
	if e := d.Decode(&votedFor); e != nil {
		panic(e)
	}
	if e := d.Decode(&logs); e != nil {
		panic(e)
	}
	rf.Debug(dPersist, "readPersist: t%d votedFor=S%d  current log: %v", currentTerm, votedFor, logs)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
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
	if rf.votedFor == -1 {
		rf.Debug(dVote, "S%v RequestVote %+v votedFor=<nil>", args.CandidateId, args)
	} else {
		rf.Debug(dVote, "S%v RequestVote %+v votedFor=S%v", args.CandidateId, args, rf.votedFor)
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
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()
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
	ConflictTerm  int // newEntry和rf.logs在什么term下冲突
	ConflictIndex int //
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

	rf.Debug(dLog, "receive AppendEntries from S%d args.term=%d %+v", args.LeaderId, args.Term, args)
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.Debug(dLog, "received term %v >= currentTerm %v from S%v, leader is legitimate", args.Term, rf.currentTerm, args.LeaderId)
		rf.state = Follower
		rf.resetTerm(args.Term)
	} else if rf.state == Follower && args.Term > rf.currentTerm {
		rf.Debug(dLog, "receive AppendEntries from S%d args.term=%d %+v", args.LeaderId, args.Term, args)
		rf.resetTerm(args.Term)
	} else if rf.state == Leader && args.Term > rf.currentTerm {
		rf.Debug(dLog, "received term %v > currentTerm %v from S%v, back to Follower", args.Term, rf.currentTerm, args.LeaderId)
		rf.state = Follower
		rf.resetTerm(args.Term)
	}

	if args.PrevLogIndex > 0 {
		if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm { // 获取到在PrevLogIndex上的entry
			rf.Debug(dLog, "log consistency check failed. local log at prev {%d t%d}: %+v  full log: %v", args.PrevLogIndex, args.PrevLogTerm, prev, rf.logs)
			if prev != nil { // 说明是Term不相等
				for _, entry := range rf.logs {
					if entry.Term == prev.Term {
						reply.ConflictIndex = entry.Index // ConflictIndex记录第一个不相等term的entry的Index
						break
					}
				}
				reply.ConflictTerm = prev.Term // ConflictTerm记录的是不相等的term
			} else { // prevLogIndex的位置为空
				reply.ConflictIndex = rf.LogTail().Index
				reply.ConflictTerm = 0
			}
			reply.Success = false
			return
		}
	}

	// 合并日志
	if len(args.Entries) > 0 {
		rf.Debug(dLog, "before merge: %s", rf.FormatLog())
		appendLeft := 0
		for i, entry := range args.Entries {
			if local := rf.GetLogAtIndex(entry.Index); local != nil {
				if local.Index != entry.Index {
					panic(rf.Sdebug(dFatal, "LMP violated: local.Index != entry.Index. entry: %+v  local log at entry.Index: %+v", entry, local))
				}
				if local.Term != entry.Term { // 如果有不一致的
					rf.Debug(dLog, "merge conflict at %d", i)
					rf.logs = rf.logs[:rf.LogIndexToSubscript(entry.Index)]
					appendLeft = i
					break
				}
				appendLeft = i + 1
			}
		}
		for i := appendLeft; i < len(args.Entries); i++ {
			entry := *args.Entries[i]
			rf.logs = append(rf.logs, &entry)
		}
		rf.Debug(dLog, "after merge: %s", rf.FormatLog())
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.LogTail().Index)
		rf.applyCond.Broadcast()
	}
	rf.Debug(dLog, "finish process heartbeat: commitIndex=%d", rf.commitIndex)
	reply.Success = true
}

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
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
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.LogTail().Index + 1,
		Command: command,
	}
	rf.logs = append(rf.logs, &entry)
	rf.persist()
	rf.matchIndex[rf.me] += 1
	rf.Debug(dClient, "client start replication with entry %v %s", entry, rf.FormatState())
	for peer := range rf.peers {
		rf.replicateCond[peer].Broadcast()
	}
	return entry.Index, rf.currentTerm, rf.state == Leader
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
			rf.votedFor = rf.me
			rf.persist()
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
	rf.Debug(dHeartbeat, "BroadcastHeartbeat start")
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.Replicate(peer)
	}
	rf.mu.Lock()
}

func (rf *Raft) DoHeartbeat() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval)
		if !rf.needHeartbeat() {
			continue
		}
		rf.mu.Lock()
		rf.BroadcastHeartbeat()
		rf.mu.Unlock()
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
	return rf.state == Leader && server != rf.me && rf.GetLogAtIndex(nextIndex) != nil && rf.LogTail().Index > nextIndex
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
	prev := rf.GetLogAtIndex(nextIndex - 1)
	rf.Debug(dWarn, "replicate S%d nextIndex=%v matchIndex=%v prevLog: %v", server, rf.nextIndex, rf.matchIndex, prev)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()
	rf.Sync(server, args) // 同步日志
}

func (rf *Raft) Sync(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Debug(dHeartbeat, "S%d AppendEntriesReply %+v rf.term=%d args.Term=%d", server, reply, rf.currentTerm, args.Term)
	if rf.currentTerm != args.Term {
		rf.Debug(dHeartbeat, "S%d AppendEntriesReply rejected since rf.term(%d) != args.Term(%d)", server, rf.currentTerm, args.Term)
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
			for i := rf.commitIndex; i <= logTailIndex; i++ {
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
			if reply.Term < rf.currentTerm {
				rf.Debug(dHeartbeat, "S%d false negative reply rejected")
				return
			}
			rf.matchIndex[server] = 0
			nextIndex := rf.nextIndex[server]
			if reply.ConflictTerm > 0 {
				for i := len(rf.logs) - 1; i >= 1; i-- {
					if rf.logs[i].Term == reply.ConflictTerm {
						rf.nextIndex[server] = Min(nextIndex, rf.logs[i].Index+1)
						rf.Debug(dHeartbeat, "S%d old_nextIndex: %d new_nextIndex: %d  full log: %s", server, nextIndex, rf.nextIndex[server], rf.FormatLog())
						return
					}
				}
			}
			rf.nextIndex[server] = Max(1, Min(nextIndex, reply.ConflictIndex))
			rf.Debug(dHeartbeat, "S%d old_nextIndex: %d new_nextIndex: %d  full log: %s", server, nextIndex, rf.nextIndex[server], rf.FormatLog())
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
		nextCommitLog := *rf.logs[rf.lastApplied]
		rf.Debug(dCommit, "apply rf[%d]=%+v", rf.lastApplied, nextCommitLog)
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
	return rf.commitIndex > rf.lastApplied
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
	rf.votedFor = -1
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
		if peer == me {
			continue
		}
		go rf.DoReplicate(peer)
	}

	if len(rf.logs) != 1 {
		panic("len(rf.log != 1)")
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
