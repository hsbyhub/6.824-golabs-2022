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
	"6.824/labgob"
	"6.824/util/logs"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	mu        deadlock.RWMutex    // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist
	currentTerm int
	votedFor    int
	logs        []*Log

	// volatile
	role        Role
	commitIndex int
	lastApplied int
	heartbeat   int64
	leaderId    int
	applyCh     chan ApplyMsg

	// volatile on leader
	nextIndex  []int
	matchIndex []int
}

type Log struct {
	Term int
	Cmd  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == RoleLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	defer func() {
		if rf.votedFor == 0 {
			rf.votedFor = -1
		}
		if rf.logs == nil {
			rf.logs = make([]*Log, 0)
		}
	}()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.currentTerm); err != nil {
		logs.Info("%v read persist raft.currentTerm error[%v]", rf.toString(), rf.currentTerm)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		logs.Info("%v read persist raft.votedFor error[%v]", rf.toString(), rf.votedFor)
	}
	if err := d.Decode(&rf.logs); err != nil {
		logs.Info("%v read persist raft.logs error[%v]", rf.toString(), rf.logs)
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	Term int
	Err  string
}

func (a *RequestVoteArgs) toString() string {
	return fmt.Sprintf("Term[%v] Candidate[%v] LastLogIndex[%v] LastLogTerm[%v]", a.Term, a.CandidateId, a.LastLogIndex, a.LastLogTerm)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
}

func (a *AppendEntriesArgs) toString() string {
	return fmt.Sprintf("Term[%v] Leader[%v] LeaderCommit[%v] PrevLogIndex[%v] PrevLogTerm[%v] LogCnt[%v]", a.Term, a.LeaderId, a.LeaderCommit, a.PrevLogIndex, a.PrevLogTerm, len(a.Entries))
}

type AppendEntriesReply struct {
	Term int
	Err  string
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Err = rf.requestVote(args)
	reply.Term = rf.currentTerm
}

// example RequestVote RPC handler.
func (rf *Raft) requestVote(args *RequestVoteArgs) string {
	// 处理任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RoleFollower
		rf.votedFor = -1
	}

	// 是否已经投票
	if rf.votedFor != -1 {
		return fmt.Sprintf("this term voted")
	}

	// 检查任期
	if args.Term < rf.currentTerm {
		return fmt.Sprintf("term too low")
	}

	// 检查日志
	if args.LastLogTerm < rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex()) {
		return fmt.Sprintf("log no up todate")
	}

	// 投票
	rf.votedFor = args.CandidateId
	rf.role = RoleCandidate

	return ""
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Err = rf.appendEntries(args)
	reply.Term = rf.currentTerm
}

func (rf *Raft) appendEntries(args *AppendEntriesArgs) string {
	// 处理任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RoleFollower
		rf.votedFor = -1
	}

	// 检查任期
	if args.Term < rf.currentTerm {
		return fmt.Sprintf("term too low")
	}

	// 刷新
	rf.role = RoleFollower
	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId
	rf.heartbeat = time.Now().UnixMilli()
	logs.Debug("%v append entries recv args[%+v] follow", rf.toString(), args.toString())

	// 检查前一条日志是否存在
	if args.PrevLogIndex > len(rf.logs) {
		return fmt.Sprintf("prev log not found")
	}

	// 检查前一条日志是否同一任期
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
		// 删除并返回错误
		rf.logs = rf.logs[:args.PrevLogIndex-1]
		return fmt.Sprintf("prev log term diff")
	}

	// 开始复制
	rf.logs = append(rf.logs[:args.PrevLogIndex], args.Entries...)

	// 开始提交
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := args.LeaderCommit
		lastLogIndex := rf.lastLogIndex()
		if lastLogIndex < commitIndex {
			commitIndex = lastLogIndex
		}
		for rf.commitIndex < commitIndex {
			rf.commitIndex++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex-1].Cmd,
				CommandIndex: rf.commitIndex,
			}
		}
		logs.Debug("%v append entries recv args[%+v] commit[%v]", rf.toString(), args.toString(), rf.commitIndex)
	}

	return ""
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.role == RoleLeader
	if !isLeader {
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, &Log{
		Term: rf.currentTerm,
		Cmd:  command,
	})
	index = rf.lastLogIndex()
	term = rf.lastLogTerm()
	go rf.OnAppendEntriesTicker()

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

// The onTicker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) onTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		go rf.OnElectionTicker()
		go rf.OnAppendEntriesTicker()
	}
}

func (rf *Raft) OnElectionTicker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 检查是否任期过期
	if rf.role == RoleFollower && time.Now().UnixMilli() > rf.heartbeat+HearbeatTimeout {
		rf.role = RoleCandidate
		logs.Debug("%v check election timeout", rf.toString())
	}

	// 检查是否为candidate
	if rf.role != RoleCandidate {
		return
	}

	// 自增任期并投票给自己
	rf.currentTerm++
	rf.votedFor = rf.me

	// 构造请求
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	cnt := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.OnElectionToServer(server, &cnt, args)
	}
}

func (rf *Raft) OnElectionToServer(server int, cnt *int, args RequestVoteArgs) {
	rf.mu.RLock()
	// 验证状态, 防止临界区外修改
	if rf.role != RoleCandidate ||
		rf.currentTerm != args.Term {
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()

	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return
	}

	// 开始验证
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 检查任期
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = RoleFollower
		logs.Debug("%v request vote to server[%v] found higher term[%v]", rf.toString(), server, reply.Term)
	}

	// 验证状态, 防止临界区外修改
	if rf.role != RoleCandidate ||
		rf.currentTerm != args.Term {
		return
	}

	// 检查是否停止
	if *cnt == -1 {
		return
	}

	// 检查是否获得投票
	if reply.Err == "" {
		*cnt++
		logs.Debug("%v request vote to server[%v] succ", rf.toString(), server)
	} else {
		logs.Debug("%v request vote to server[%v] error[%v]", rf.toString(), server, reply.Err)
		return
	}

	// 检查是否足够票数
	if *cnt <= len(rf.peers)/2 {
		return
	}

	// 选举成功, 初始化leader并周知其它服务
	rf.role = RoleLeader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	logs.Debug("%v get vote[%v] election success", rf.toString(), *cnt)
	*cnt = -1
	go rf.OnAppendEntriesTicker()
}

func (rf *Raft) OnAppendEntriesTicker() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	// 检查是否为leader
	if rf.role != RoleLeader {
		return
	}

	// 构造请求
	var args = AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	// 向其他节点复制日志
	cnt := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.OnAppendEntriesToServer(server, &cnt, args, rf.lastLogIndex())
	}

}

func (rf *Raft) OnAppendEntriesToServer(server int, cnt *int, args AppendEntriesArgs, index int) {
	for {
		isRetry, msg := rf.AppendEntriesToServerHandle(server, cnt, &args, index)
		logs.Debug("%v on append to server handle args[%v] index[%v] msg[%v]", rf.toString(), args.toString(), index, msg)
		if !isRetry {
			break
		}
	}
}

func (rf *Raft) AppendEntriesToServerHandle(server int, cnt *int, args *AppendEntriesArgs, index int) (bool, string) {
	rf.mu.RLock()

	// 验证状态, 防止临界区外修改
	if rf.role != RoleLeader ||
		rf.lastLogIndex() < index {
		rf.mu.RUnlock()
		return false, "state modified"
	}

	// 构造请求
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex > index {
		rf.mu.RUnlock()
		return false, "state modified"
	}
	args.PrevLogTerm = -1
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
	}
	args.Entries = rf.logs[args.PrevLogIndex:index]
	rf.mu.RUnlock()

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return true, "network invalid"
	}

	// 开始验证
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 检查任期
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = RoleFollower
		return false, "found higher term"
	}

	// 验证状态, 防止临界区外修改
	if rf.role != RoleLeader ||
		rf.lastLogIndex() < index {
		return false, "state modified"
	}

	// 检查是否复制
	if reply.Err != "" {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
			return true, fmt.Sprintf("replicate error[%v]", reply.Err)
		}
		return false, fmt.Sprintf("replicate error[%v]", reply.Err)
	}

	// 复制成功
	rf.nextIndex[server] = index + 1
	rf.matchIndex[server] = index

	// 检查是否可以提交
	if *cnt == -1 {
		return false, "replicate success and alreay commit"
	}
	*cnt++
	if *cnt <= len(rf.peers)/2 {
		return false, fmt.Sprintf("replicate success and count[%v] not enough", *cnt)
	}
	*cnt = -1

	// 提交
	if rf.commitIndex < index {
		for rf.commitIndex < index {
			rf.commitIndex++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex-1].Cmd,
				CommandIndex: rf.commitIndex,
			}
		}
		// 周知
		go rf.OnAppendEntriesTicker()
		return false, fmt.Sprintf("replicate index[%v] succcess and commit index[%v] success", index, rf.commitIndex)
	}

	return false, fmt.Sprintf("replicate index[%v] succcess", index)
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs)
}

func (rf *Raft) lastLogTerm() int {
	lastLogTerm := -1
	if rf.lastLogIndex() > 0 {
		lastLogTerm = rf.logs[rf.lastLogIndex()-1].Term
	}
	return lastLogTerm
}

func (rf *Raft) toString() string {
	//vs := make([]string, len(rf.logs))
	//for i, v := range rf.logs {
	//	vs[i] = fmt.Sprintf("%v:%v:%v", i+1, v.Term, v.Cmd)
	//}
	return fmt.Sprintf("SRV[%v] ROLE[%v] TERM[%v] COMMIT[%v] LI[%v] LT[%v]", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastLogIndex(), rf.lastLogTerm())
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
	// persist
	//rf.currentTerm = 0
	//rf.votedFor = -1
	//rf.logs = make([]*Log, 0)

	// volatile
	rf.role = RoleFollower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// volatile on leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start onTicker goroutine to start elections
	go rf.onTicker()

	return rf
}
