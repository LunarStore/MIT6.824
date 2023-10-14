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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// table map[uint]map[uint]uint
	electTimer     *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	wakeupApply    chan interface{}
	state          uint //当前服务器扮演的角色

	// Persistent state on all servers:
	currentTerm int32
	votedFor    int32
	logs        []LogEntry

	// Volatile state on all servers:
	commitIndex int32
	lastApplied int32

	// Volatile state on leaders:
	nextIndex  []int32
	matchIndex []int32

	// logFile *os.File
}

// fmt.Printf("[@id=%v state=%v at %v] send AppendEntries\n", rf.me, rf.state, rf.currentTerm)

func (rf *Raft) DebugPrint(event string, a ...interface{}) {
	if DEBUG == true {
		msg := fmt.Sprintf(event, a...)
		fmt.Printf("[@id=%v state=%v at %v] event:%s\n", rf.me, rf.state, rf.currentTerm, msg)
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
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

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
	if d.Decode(&ct) != nil ||
		d.Decode(&vf) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("data Decode fail!<%v>\n", data)
	} else {
		rf.currentTerm = ct
		rf.votedFor = vf
		rf.logs = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 //变成Follower重新拥有一次投票权
		rf.persist()

		rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	} else if args.Term < rf.currentTerm { //当前状态有可能是Follower、Leader，所以最好不要改变定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electTimer.Reset(RandomTimeout())
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

	if args.PrevLogIndex >= int32(len(rf.logs)) {
		reply.ConflictIndex = int32(len(rf.logs))
		reply.Success = false
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		idx := args.PrevLogIndex
		for rf.logs[idx-1].Term == rf.logs[args.PrevLogIndex].Term { //找冲突时期第一个index
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
	for pCur < len(rf.logs) && pAdd < len(args.Entries) {
		if rf.logs[pCur].Term == args.Entries[pAdd].Term { //一致不用看
			pCur++
			pAdd++
			continue
		} else {
			break
		}
	}

	if pCur < len(rf.logs) && pAdd < len(args.Entries) { //存在不一致的log
		rf.logs = append(rf.logs[:pCur], args.Entries[pAdd:]...)
		rf.persist()
	} else if pCur < len(rf.logs) { // len(args.Entries)太短
		rf.logs = rf.logs[:pCur] // is need?，删除无影响
		rf.persist()
	} else if pAdd < len(args.Entries) { //新增log
		rf.logs = append(rf.logs, args.Entries[pAdd:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > int32(len(rf.logs)-1) { //args.PrevLogIndex + len(args.Entries)
			rf.commitIndex = int32(len(rf.logs) - 1) //args.PrevLogIndex + len(args.Entries)
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		rf.ApplyToStateMachine()
		// rf.wakeupApply <- rf.commitIndex
	}
	rf.DebugPrint("me commit=%v, len(logs)=%v", rf.commitIndex, len(rf.logs))
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// 为了性能，应该另起一个协程
func (rf *Raft) ApplyToStateMachine() {

	// for rf.killed() == false {
	// 	select {
	// 	case cmitIndex := <-rf.wakeupApply:
	// 		for cmitIndex.(int32) > rf.lastApplied { //仅此一处访问，可以不加锁？？？
	// 			rf.lastApplied++
	// 			rf.applyCh <- rf.logs[rf.lastApplied].Command
	// 		}
	// 	}
	// }
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- rf.logs[rf.lastApplied].Command
	}
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
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 //变成Follower重新拥有一次投票权
		rf.persist()

		rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId)) &&
		(rf.logs[len(rf.logs)-1].Term < args.LastLogTerm || rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && (len(rf.logs)-1) <= int(args.LastLogIndex)) {
		rf.votedFor = int32(args.CandidateId)
		rf.persist()

		// fmt.Printf("%v vote to %v at %v\n", rf.me, args.CandidateId, reply.Term)

		rf.DebugPrint("me vote to %v", args.CandidateId)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
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
		LastLogIndex: int32(len(rf.logs) - 1),
		LastLogTerm:  int32(rf.logs[len(rf.logs)-1].Term),
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
				rf.currentTerm = reqVoteReply.Term
				rf.state = Follower
				rf.votedFor = -1 //变成Follower重新拥有一次投票权
				rf.persist()

				rf.electTimer.Reset(RandomTimeout())
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
						rf.nextIndex[i] = int32(len(rf.logs))
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
		rf.DebugPrint("send heartbeat")
	} else { //追加日志
		rf.DebugPrint("send AppendEntries <commit=%v, command=%v>", rf.commitIndex, command)
		rf.logs = append(rf.logs, LogEntry{
			Command: ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: len(rf.logs),
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
				reqAppendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: int32(rf.nextIndex[id] - 1),
					PrevLogTerm:  rf.logs[rf.nextIndex[id]-1].Term,
					Entries:      rf.logs[rf.nextIndex[id]:], //[nextIndex, end)
					LeaderCommit: rf.commitIndex,
				}
				reqAppendEntriesReply := AppendEntriesReply{}

				rf.mu.Unlock()
				ok := rf.sendRequestAppendEntries(id, &reqAppendEntriesArgs, &reqAppendEntriesReply)
				if ok != true {
					rf.DebugPrint("id=%v is crash!!", id)
					rf.mu.Lock()
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
					//change to follower
					rf.currentTerm = reqAppendEntriesReply.Term
					rf.state = Follower
					rf.votedFor = -1 //变成Follower重新拥有一次投票权
					rf.persist()

					rf.electTimer.Reset(RandomTimeout())
					rf.heartbeatTimer.Stop()

					break //变成Follower，放弃追加日志
				} else if rf.currentTerm == reqAppendEntriesReply.Term {

					//心跳不算日志，不会改变logs的长度
					//...
					if reqAppendEntriesReply.Success == true { //对该Follower追加日志成功
						matchIndex := reqAppendEntriesArgs.PrevLogIndex + int32(len(reqAppendEntriesArgs.Entries))

						if matchIndex+1 > rf.nextIndex[id] {
							rf.nextIndex[id] = matchIndex + 1 //len(rf.logs)
							rf.matchIndex[id] = matchIndex
						}

					} else {
						//rf.nextIndex[id]-- //这种方式，当存在冗余的回复的时候就会对一处不匹配进行多次自减
						//rf.nextIndex[id] = reqAppendEntriesArgs.PrevLogIndex

						//检测ConflictIndex是否为-1
						rf.nextIndex[id] = reqAppendEntriesReply.ConflictIndex
					}
				}

				if len(rf.logs)-1 < int(rf.nextIndex[id]) {
					break
				}
			}

			//在matchIndex中找一个可以更新commitIndex的数N
			for _, N := range rf.matchIndex {
				if N <= rf.commitIndex || rf.logs[N].Term != rf.currentTerm {
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
					rf.ApplyToStateMachine()
					// rf.wakeupApply <- rf.commitIndex
					rf.DebugPrint("--------------commit=%v", rf.commitIndex)
				}
			}
		}(i)
	}

	return int(rf.currentTerm), true, len(rf.logs) - 1
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
		electTimer:     time.NewTimer(RandomTimeout()), //选举超时定时器
		heartbeatTimer: time.NewTimer(0),               //心跳超时定时器
		applyCh:        applyCh,
		wakeupApply:    make(chan interface{}, 10),
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int32, len(peers)),
		matchIndex:     make([]int32, len(peers)),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.heartbeatTimer.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.EventHandle()
	// go rf.ApplyToStateMachine()

	return rf
}
