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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	CommandTerm  int32
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Command ApplyMsg
	Term    int32
}

var DEBUG bool = false

const (
	//state
	Follower = iota
	Candidate
	Leader
)

const (
	//event
	ELECTTIMEOUT = iota
	RCVHTERM
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	toApply bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// table map[uint]map[uint]uint
	electTimer     *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	wakeupApply    chan interface{}
	wakeupSnapshot chan ApplyMsg
	state          uint //当前服务器扮演的角色

	// Persistent state on all servers:
	currentTerm      int32
	votedFor         int32
	logs             []LogEntry
	lastIncludeIndex int32
	lastIncludeTerm  int32
	// Volatile state on all servers:
	commitIndex int32
	lastApplied int32

	// Volatile state on leaders:
	nextIndex  []int32
	matchIndex []int32

	snapshotCount int32
	// logFile *os.File
}

func (rf *Raft) GetLog(index int32) LogEntry {
	if index-(rf.lastIncludeIndex+1) < 0 {
		return LogEntry{
			Term: rf.lastIncludeTerm, //command是否需要填充？
		}
	}
	return rf.logs[index-(rf.lastIncludeIndex+1)]
}
func (rf *Raft) GetLength() int32 {
	return rf.lastIncludeIndex + 1 + int32(len(rf.logs))
}
func (rf *Raft) GetRealLength() int32 {
	return int32(len(rf.logs))
}

func (rf *Raft) Slice(begin int, end int) []LogEntry {
	if begin != -1 && end != -1 {
		return rf.logs[begin-(int(rf.lastIncludeIndex)+1) : end-(int(rf.lastIncludeIndex)+1)]
	} else if begin != -1 {
		return rf.logs[begin-(int(rf.lastIncludeIndex)+1):]
	} else if end != -1 {
		return rf.logs[:end-(int(rf.lastIncludeIndex)+1)]
	} else {
		return rf.logs
	}
}

// fmt.Printf("[@id=%v state=%v at %v] send AppendEntries\n", rf.me, rf.state, rf.currentTerm)

func (rf *Raft) DebugPrint(event string, a ...interface{}) {
	if DEBUG == true {
		msg := fmt.Sprintf(event, a...)
		fmt.Printf("[@id=%v state=%v at term = %v votefor %v lastIncludeIndex = %v lastIncludeTerm = %v commitIndex = %v lastApplied = %v len(logs) = %v] event:%s \n",
			rf.me,
			rf.state,
			rf.currentTerm,
			rf.votedFor,
			rf.lastIncludeIndex,
			rf.lastIncludeTerm,
			rf.commitIndex,
			rf.lastApplied,
			len(rf.logs),
			msg)
	}
	// fmt.Fprintf(rf.logFile, "[@id=%v state=%v at %v] event:%s\n", rf.me, rf.state, rf.currentTerm, msg)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = int(rf.currentTerm)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	raftstate := w.Bytes()

	rf.persister.SaveState(raftstate)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ct int32
	var vf int32
	var logs []LogEntry
	var lii int32
	var lit int32
	if d.Decode(&ct) != nil ||
		d.Decode(&vf) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lii) != nil ||
		d.Decode(&lit) != nil {
		log.Fatalf("data Decode fail!<%v>\n", data)
	} else {
		rf.currentTerm = ct
		rf.votedFor = vf
		rf.logs = logs
		rf.lastIncludeIndex = lii
		rf.lastIncludeTerm = lit
	}
}

func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

func (rf *Raft) ReadSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) { //上层应用调用Snapshot时，同时有个协程正在持有锁去向上层apply数据（上层应用因为调用Snapshot阻塞无法响应applyCh），此时会发生死锁。
	// Your code here (2D).

	rf.mu.Lock()
	rf.DebugPrint("manual Snapshot start")
	defer rf.mu.Unlock()

	if index <= int(rf.lastIncludeIndex) || rf.snapshotCount != 0 { //is need equal?//保证快照应用的原子性
		return
	}

	// 更新快照日志
	lastSnapshotLog := rf.GetLog(int32(index)) //GetLog和lastIncludeIndex存在依赖
	tailLogs := make([]LogEntry, 0)
	for i := index + 1; i < int(rf.GetLength()); i++ {
		tailLogs = append(tailLogs, rf.GetLog(int32(i)))
	}

	rf.lastIncludeIndex = int32(index)
	rf.lastIncludeTerm = lastSnapshotLog.Term
	rf.logs = tailLogs

	if int32(index) > rf.commitIndex {
		rf.commitIndex = int32(index)
	}
	if int32(index) > rf.lastApplied {
		rf.lastApplied = int32(index)
	}
	rf.persist()
	rf.persister.SaveSnapshot(snapshot)

	rf.DebugPrint("manual Snapshot end")
	return
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term          int32
	ConflictIndex int32
	Success       bool
}

// example RequestAppendEntries RPC handler.
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.ConflictIndex = -1
	reply.Success = false
	reply.Term = -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		//change to follower
		if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
			rf.electTimer.Reset(RandomTimeout())
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = int32(args.LeaderId) // rf.votedFor = -1 //变成Follower重新拥有一次投票权
		rf.persist()

		//cc rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	} else if args.Term < rf.currentTerm { //当前状态有可能是Follower、Leader，所以最好不要改变定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electTimer.Reset(RandomTimeout()) //这里直接重置定时器的话会导致TestFigure8Unreliable2C无法达成一致而失败，因为后面是有判断当前追加日志的请求是否有效，无效的会直接返回！！！
	reply.Term = rf.currentTerm

	if args.Entries == nil { //	收到心跳

		reply.Success = true

		rf.DebugPrint("recerive heartbeat <args: term=%v, LeaderId=%v, prevLogIndex=%v, preLogTerm=%v, LeaderCommit=%v>", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		// return
	} else { //追加日志通知
		rf.DebugPrint("recerive AppendEntries <args: term=%v, LeaderId=%v, prevLogIndex=%v, preLogTerm=%v, LeaderCommit=%v, EntriesLength=%v>", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	}

	// if args.PrevLogIndex >= int32(len(rf.logs)) ||
	// 	rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	return
	// } else {
	// 	reply.Success = true
	// }
	if args.PrevLogIndex < rf.lastIncludeIndex { //add3
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludeIndex + 1
		return
	}
	if args.PrevLogIndex >= rf.GetLength() {
		reply.ConflictIndex = rf.GetLength()
		reply.Success = false
		return
	} else if rf.GetLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		idx := args.PrevLogIndex
		for idx > rf.lastIncludeIndex && rf.GetLog(idx-1).Term == rf.GetLog(args.PrevLogIndex).Term { //找冲突时期第一个index，日志被压缩的话返回最后一条被压缩的日志的下标
			idx--
		}
		reply.ConflictIndex = idx
		reply.Success = false
		return
	} else {
		reply.Success = true
	}

	pAdd := 0
	pCur := int(args.PrevLogIndex + 1)
	for pCur < int(rf.GetLength()) && pAdd < len(args.Entries) {
		if rf.GetLog(int32(pCur)).Term == args.Entries[pAdd].Term { //一致不用看
			pCur++
			pAdd++
			continue
		} else {
			break
		}
	}

	if pCur < int(rf.GetLength()) && pAdd < len(args.Entries) { //存在不一致的log
		rf.logs = append(rf.Slice(-1, pCur), args.Entries[pAdd:]...)
		rf.persist()
	} else if pCur < int(rf.GetLength()) { // len(args.Entries)太短
		// rf.logs = rf.Slice(-1, pCur) // is need?，删除无影响	原本有多余的不要删除！！！
		// rf.persist()
	} else if pAdd < len(args.Entries) { //新增log
		rf.logs = append(rf.Slice(-1, -1), args.Entries[pAdd:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		// if args.LeaderCommit > rf.GetLength()-1 { //args.PrevLogIndex + len(args.Entries)
		// 	rf.commitIndex = rf.GetLength() - 1 //args.PrevLogIndex + len(args.Entries)
		// } else {
		// 	rf.commitIndex = args.LeaderCommit
		// }
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+int32(len(args.Entries)))

		rf.DebugPrint("will to apply AppendEntries <args: term=%v, LeaderId=%v, prevLogIndex=%v, preLogTerm=%v, LeaderCommit=%v, EntriesLength=%v>", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
		// rf.ApplyToStateMachine()
		//非阻塞写
		select {
		case rf.wakeupApply <- rf.commitIndex:
		default:
		}

	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func Max(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a int32, b int32) int32 {
	if a < b {
		return a
	} else {
		return b
	}
}

// 为了性能，应该另起一个协程
func (rf *Raft) ApplyToStateMachine() {

	for rf.killed() == false {
		select {
		case <-rf.wakeupApply:
			// if rf.toApply == false {
			// 	continue
			// }
			rf.mu.Lock()
			if rf.snapshotCount != 0 { //保证快照应用的原子性
				rf.mu.Unlock()
				continue
			}
			commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
			entries := make([]LogEntry, commitIndex-lastApplied)
			rf.DebugPrint("begin apply len(logs) = %v len(ApplyEntries) = %v\n", len(rf.logs), len(entries))
			copy(entries, rf.Slice(int(lastApplied+1), int(commitIndex+1))) //+1
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- entry.Command
			}
			rf.mu.Lock()
			if rf.snapshotCount != 0 { //保证快照应用的原子性
				rf.mu.Unlock()
				continue
			}
			rf.DebugPrint("end apply rf.lastApplied = MAX(rf.lastApplied = %v, commitIndex = %v)\n", rf.lastApplied, commitIndex)
			rf.lastApplied = Max(rf.lastApplied, commitIndex)
			rf.mu.Unlock()
		case msg := <-rf.wakeupSnapshot:
			rf.mu.Lock()
			rf.DebugPrint("InstallSnapshot applying, SnapshotIndex = %v SnapshotTerm = %v", msg.SnapshotIndex, msg.SnapshotTerm)
			rf.mu.Unlock()
			rf.applyCh <- msg

			rf.mu.Lock()
			rf.snapshotCount-- //保证快照应用的原子性
			rf.mu.Unlock()
		}
	}
	// for rf.commitIndex > rf.lastApplied {
	// 	rf.lastApplied++
	// 	rf.applyCh <- rf.GetLog(rf.lastApplied).Command //rf.logs[rf.lastApplied].Command
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if rf.currentTerm < args.Term {
		//change to follower
		if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
			rf.electTimer.Reset(RandomTimeout())
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 //变成Follower重新拥有一次投票权
		rf.persist()

		//cc rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId)) &&
		(rf.GetLog(rf.GetLength()-1).Term < args.LastLogTerm || rf.GetLog(rf.GetLength()-1).Term == args.LastLogTerm && int(rf.GetLength()-1) <= int(args.LastLogIndex)) {
		rf.electTimer.Reset(RandomTimeout()) //Student Guide suguesst
		rf.votedFor = int32(args.CandidateId)
		rf.persist()

		// fmt.Printf("%v vote to %v at %v\n", rf.me, args.CandidateId, reply.Term)

		rf.DebugPrint("me vote to %v", args.CandidateId)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type InstallSnapshotArgs struct {
	Term             int32
	LeaderId         int32
	LastIncludeIndex int32
	LastIncludeTerm  int32
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int32
}

// 和Snapshot函数差不多
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //我的时期更新或者我的快照更新注意等号！！！
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
			rf.electTimer.Reset(RandomTimeout())
		}
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId // rf.votedFor = -1 //变成Follower重新拥有一次投票权
		rf.persist()

		//cc rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	}

	reply.Term = rf.currentTerm
	// if args.LastIncludeIndex <= rf.commitIndex {
	// 	return
	// }
	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		return
	}
	//保留一致的log entries
	tailLogs := make([]LogEntry, 0)
	if args.LastIncludeIndex < rf.GetLength() && args.LastIncludeTerm == rf.GetLog(args.LastIncludeIndex).Term {
		for i := args.LastIncludeIndex + 1; i < rf.GetLength(); i++ {
			tailLogs = append(tailLogs, rf.GetLog(int32(i)))
		}
	}

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.logs = tailLogs

	if rf.commitIndex < args.LastIncludeIndex { // is need?
		rf.commitIndex = args.LastIncludeIndex
	}
	// if rf.lastApplied < args.LastIncludeIndex {

	// 	rf.lastApplied = args.LastIncludeIndex
	// }

	rf.lastApplied = args.LastIncludeIndex //无条件更改

	//持久化状态和快照
	rf.persist()
	rf.persister.SaveSnapshot(args.Data)
	rf.DebugPrint("InstallSnapshot apply args=<Term = %v LeaderId = %v LastIncludeIndex = %v LastIncludeTerm = %v>",
		args.Term,
		args.LeaderId,
		args.LastIncludeIndex,
		args.LastIncludeTerm)

	rf.snapshotCount++ //保证快照应用的原子性
	rf.mu.Unlock()
	//应用快照
	rf.wakeupSnapshot <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludeTerm),
		SnapshotIndex: int(args.LastIncludeIndex),
	}

	rf.mu.Lock()
	return
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	term, isLeader, index = rf.StartAppendEntries(command)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// func (rf *Raft) ChangeState()
// voteFor重置问题/sendRequestVote延迟太高，超时再提交结果的处理，提交term不一致问题
func (rf *Raft) StartElection(timeout time.Duration) {
	//对每个对等方发起拉票请求
	//每次开始选举的固定初始化
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	//增加term
	rf.currentTerm++
	rf.votedFor = int32(rf.me)
	rf.electTimer.Reset(timeout)
	rf.heartbeatTimer.Stop() //real need?
	rf.persist()

	voteNumber := int32(1) //go不用关心释放问题

	// fmt.Printf("%v current term == %v\n", rf.me, rf.currentTerm)
	rf.DebugPrint("start elect")
	reqVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLength() - 1,                 //int32(len(rf.logs) - 1)
		LastLogTerm:  rf.GetLog(rf.GetLength() - 1).Term, //int32(rf.logs[len(rf.logs)-1].Term)
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//并发的去拉票，而非串行去拉票
		go func(id int) {
			reqVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, &reqVoteArgs, &reqVoteReply)
			if ok != true {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reqVoteArgs.Term != rf.currentTerm { //同一个RPC报文要仅在同一个Term内有效
				return
			}
			if rf.currentTerm < reqVoteReply.Term { //出现新任期，（新Leader
				//change to follower
				if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
					rf.electTimer.Reset(RandomTimeout())
				}
				rf.currentTerm = reqVoteReply.Term
				rf.state = Follower
				rf.votedFor = -1 //变成Follower重新拥有一次投票权
				rf.persist()

				//cc rf.electTimer.Reset(RandomTimeout())
				rf.heartbeatTimer.Stop()

				// log.Fatalf("cannot appear new Leader!!! arg: %v\nreply: %v\n", reqVoteArgs, reqVoteReply)
			} else if rf.currentTerm == reqVoteReply.Term {
				if rf.state == Leader {
					return
				}
				if reqVoteReply.VoteGranted == true {
					atomic.AddInt32(&voteNumber, 1)
				}

				if atomic.LoadInt32(&voteNumber) > int32(len(rf.peers)/2) {
					//获得大半数投票，成为领导
					rf.state = Leader
					//to send first heartbeat
					rf.electTimer.Stop() //Leader没有选举超时定时器
					rf.heartbeatTimer.Reset(0)

					// fmt.Printf("term == %v id == %v become leader\n", rf.currentTerm, rf.me)
					rf.DebugPrint("become leader")
					//初始化nextIndex、matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.GetLength()
						rf.matchIndex[i] = 0
					}
				}
			} //else rf.currentTerm > reqVoteReply.Term 不管
		}(i)
	}
}

// 合并Heartbeat和LogAppender
func (rf *Raft) StartAppendEntries(command interface{}) (int, bool, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return int(rf.currentTerm), false, -1
	}
	rf.heartbeatTimer.Reset(150 * time.Millisecond) //下次心跳	//每发一次日志，就当作是一次主动心跳，重置定时器
	if command == nil {                             //心跳
		// rf.DebugPrint("send heartbeat")
	} else { //追加日志
		// rf.DebugPrint("send AppendEntries <commit=%v, command=%v>", rf.commitIndex, command)
		rf.logs = append(rf.logs, LogEntry{
			Command: ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: int(rf.GetLength()),
				CommandTerm:  rf.currentTerm,
			},
			Term: rf.currentTerm,
		})

		rf.persist()
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(id int) { // do while
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for { //len(rf.logs)-1 >= int(rf.nextIndex[id])
				if rf.state != Leader {
					return
				}

				if rf.nextIndex[id]-1 < rf.lastIncludeIndex { //需要更新快照
					rf.DebugPrint("send to %v install snapshot rpc", id)
					//发送install snapshot rpc
					reqInstallSnapshotArgs := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         int32(rf.me),
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludeTerm,
						Data:             rf.persister.ReadSnapshot(),
					}

					reqInstallSnapshotReply := InstallSnapshotReply{}
					rf.mu.Unlock()
					ok := rf.sendInstallSnapShot(id, &reqInstallSnapshotArgs, &reqInstallSnapshotReply)
					if ok != true {
						rf.mu.Lock()
						rf.DebugPrint("id=%v is crash!!", id)
						return
					}

					rf.mu.Lock()
					if reqInstallSnapshotArgs.Term != rf.currentTerm { //同一个RPC报文要仅在同一个Term内有效
						return
					}

					if rf.currentTerm < reqInstallSnapshotReply.Term { //出现新任期，（新Leader
						//change to follower
						if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
							rf.electTimer.Reset(RandomTimeout())
						}
						rf.currentTerm = reqInstallSnapshotReply.Term
						rf.state = Follower
						rf.votedFor = -1 //变成Follower重新拥有一次投票权
						rf.persist()

						//cc rf.electTimer.Reset(RandomTimeout())
						rf.heartbeatTimer.Stop()

						return //变成Follower，放弃追加日志
					} else if rf.currentTerm == reqInstallSnapshotReply.Term { //任期一致更新rf.nextIndex[id]
						// rf.nextIndex[id] = rf.lastIncludeIndex + 1
						//rf.DebugPrint("update nextIndex [%v, %v] %v --- %v\n", rf.nextIndex[id], rf.GetLength()-1, rf.lastIncludeIndex, reqInstallSnapshotArgs.LastIncludeIndex)

						// rf.nextIndex[id] = Max(reqInstallSnapshotArgs.LastIncludeIndex+1, rf.lastIncludeIndex+1)
						rf.nextIndex[id] = reqInstallSnapshotArgs.LastIncludeIndex + 1

					} else {
						return
					}
				} else { //直接发日志
					rf.DebugPrint("send to %v append entries nextIndex[id] = %v logmaxindex = %v\n", id, rf.nextIndex[id], rf.GetLength()-1)
					reqAppendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: int32(rf.nextIndex[id] - 1),
						PrevLogTerm:  rf.GetLog(rf.nextIndex[id] - 1).Term, // rf.logs[rf.nextIndex[id]-1].Term,	//rf.GetLog(rf.nextIndex[id]-1).Term
						Entries:      rf.Slice(int(rf.nextIndex[id]), -1),  //[nextIndex, end)	rf.logs[rf.nextIndex[id]:]
						LeaderCommit: rf.commitIndex,
					}
					reqAppendEntriesReply := AppendEntriesReply{}
					rf.mu.Unlock()
					ok := rf.sendRequestAppendEntries(id, &reqAppendEntriesArgs, &reqAppendEntriesReply)
					if ok != true {
						rf.mu.Lock()
						rf.DebugPrint("id=%v is crash!!", id)
						return
					}
					rf.mu.Lock()
					if reqAppendEntriesArgs.Term != rf.currentTerm { //同一个RPC报文要仅在同一个Term内有效
						return
					}
					// rf.mu.Lock()
					//replyRPC的Term只会大于等于currentTerm
					if rf.currentTerm < reqAppendEntriesReply.Term { //出现新任期，（新Leader
						if reqAppendEntriesReply.Success == true {
							log.Fatal("reqAppendEntriesReply.Success should false")
						}
						//rf.DebugPrint("id=%v is change to follower", id)
						//change to follower
						if rf.state == Leader { // leader 不具备选举超时，在更换状态时，需开启选举超时
							rf.electTimer.Reset(RandomTimeout())
						}
						rf.currentTerm = reqAppendEntriesReply.Term
						rf.state = Follower
						rf.votedFor = -1 //变成Follower重新拥有一次投票权
						rf.persist()

						//cc rf.electTimer.Reset(RandomTimeout())
						rf.heartbeatTimer.Stop()

						return //变成Follower，放弃追加日志
					} else if rf.currentTerm == reqAppendEntriesReply.Term {

						//心跳不算日志，不会改变logs的长度
						//...
						if reqAppendEntriesReply.Success == true { //对该Follower追加日志成功
							matchIndex := reqAppendEntriesArgs.PrevLogIndex + int32(len(reqAppendEntriesArgs.Entries))
							//rf.DebugPrint("id=%v is append success11", id)
							if matchIndex+1 > rf.nextIndex[id] {
								//rf.DebugPrint("id=%v is append success22", id)

								rf.nextIndex[id] = matchIndex + 1 //len(rf.logs)
								rf.matchIndex[id] = matchIndex
							}

						} else {
							//rf.nextIndex[id]-- //这种方式，当存在冗余的回复的时候就会对一处不匹配进行多次自减
							//rf.nextIndex[id] = reqAppendEntriesArgs.PrevLogIndex
							//检测ConflictIndex是否为-1
							//rf.DebugPrint("id=%v is append fail", id)
							if reqAppendEntriesReply.ConflictIndex == -1 {
								log.Fatalf("ConflictIndex == -1 !!!!!!")
							}
							rf.nextIndex[id] = reqAppendEntriesReply.ConflictIndex

						}

						/*------------快照部分----------------*/

						/*------------快照部分----------------*/
					} else {
						return
					}
				}

				if int(rf.GetLength())-1 < int(rf.nextIndex[id]) {
					break
				}
			}

			//在matchIndex中找一个可以更新commitIndex的数N
			for _, N := range rf.matchIndex {
				if N <= rf.commitIndex || rf.GetLog(N).Term != rf.currentTerm {
					continue
				}

				count := 1 //leader自身也算入大于N的行列，虽然并不大于N
				for _, n := range rf.matchIndex {
					if n >= N {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					// rf.ApplyToStateMachine()
					// rf.wakeupApply <- rf.commitIndex
					select {
					case rf.wakeupApply <- rf.commitIndex:
					default:
					}
					rf.DebugPrint("--------------commit=%v", rf.commitIndex)
				}
			}

			return
		}(i)
	}

	return int(rf.currentTerm), true, int(rf.GetLength()) - 1
}

func RandomTimeout() time.Duration {
	return time.Duration(800+(rand.Int63()%150)) * time.Millisecond
}

func (rf *Raft) EventHandle() { //被动触发
	for rf.killed() == false {
		select {
		case <-rf.electTimer.C:
			//选举超时只能发生在Follower和Candidate中
			rf.mu.Lock()
			switch rf.state { // is thread safe?
			case Follower:
				//遇到选举定时器超时，转换成Candidate，重置定时器，开始选举
				rf.mu.Unlock()
				rf.StartElection(RandomTimeout())
			case Candidate:
				rf.mu.Unlock()
				rf.StartElection(RandomTimeout())
			case Leader:
				fmt.Printf("Leader cannot ElectTimeOut!\n")
				rf.electTimer.Stop()
				rf.mu.Unlock()
			default:
				log.Fatalf("rf.state == %v error state!\n", rf.state)
			}
		case <-rf.heartbeatTimer.C:
			rf.StartAppendEntries(nil)
		}
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
	rf := &Raft{
		electTimer:       time.NewTimer(RandomTimeout()), //选举超时定时器
		heartbeatTimer:   time.NewTimer(0),               //心跳超时定时器
		applyCh:          applyCh,
		wakeupApply:      make(chan interface{}, 5),
		toApply:          true,
		wakeupSnapshot:   make(chan ApplyMsg, 5),
		state:            Follower,
		currentTerm:      0,
		votedFor:         -1,
		logs:             make([]LogEntry, 0),
		lastIncludeIndex: 0,
		lastIncludeTerm:  0,
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make([]int32, len(peers)),
		matchIndex:       make([]int32, len(peers)),
		snapshotCount:    0,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.heartbeatTimer.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludeIndex ////add3
	rf.lastApplied = rf.lastIncludeIndex ////add3
	// start ticker goroutine to start elections
	go rf.EventHandle()
	go rf.ApplyToStateMachine()
	// rf.DebugPrint("me = %v\n", me)
	return rf
}
