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

	"fmt"
	"log"
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
type LogEntry struct {
	Command ApplyMsg
	Term    int32
}

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

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

// example RequestAppendEntries RPC handler.
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		//change to follower
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 //变成Follower重新拥有一次投票权

		rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	} else if args.Term < rf.currentTerm { //当前状态有可能是Follower、Leader，所以最好不要改变定时器
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Entries == nil { //	收到心跳
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.electTimer.Reset(RandomTimeout())
		// rf.heartbeatTimer.Stop()

		fmt.Printf("%v receive heartbeat at %v\n", rf.me, rf.currentTerm)
		return
	} else { //追加日志通知
		log.Fatalf("Lab2A never reach!")
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
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

		rf.electTimer.Reset(RandomTimeout())
		rf.heartbeatTimer.Stop()
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId)) &&
		(rf.logs[len(rf.logs)-1].Term < args.LastLogTerm || rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && (len(rf.logs)-1) <= int(args.LastLogIndex)) {
		rf.votedFor = int32(args.CandidateId)

		fmt.Printf("%v vote to %v at %v\n", rf.me, args.CandidateId, reply.Term)
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
	voteNumber := int32(1)   //go不用关心释放问题

	fmt.Printf("%v current term == %v\n", rf.me, rf.currentTerm)
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
			rf.sendRequestVote(id, &reqVoteArgs, &reqVoteReply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reqVoteReply.Term { //出现新任期，（新Leader
				//change to follower
				rf.currentTerm = reqVoteReply.Term
				rf.state = Follower
				rf.votedFor = -1 //变成Follower重新拥有一次投票权

				rf.electTimer.Reset(RandomTimeout())
				rf.heartbeatTimer.Stop()

				log.Fatalf("cannot appear new Leader!!! arg: %v\nreply: %v\n", reqVoteArgs, reqVoteReply)
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

					fmt.Printf("term == %v id == %v become leader\n", rf.currentTerm, rf.me)
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

func (rf *Raft) Heartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatTimer.Reset(150 * time.Millisecond) //下次心跳

	fmt.Printf("%v current term == %v send heartbeat\n", rf.me, rf.currentTerm)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		reqAppendEntriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: int32(rf.nextIndex[i] - 1),
			PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		go func(id int, reqAppendArgs AppendEntriesArgs) {
			reqAppendEntriesReply := AppendEntriesReply{}

			rf.sendRequestAppendEntries(id, &reqAppendArgs, &reqAppendEntriesReply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reqAppendEntriesReply.Term { //出现新任期，（新Leader
				if reqAppendEntriesReply.Success == true {
					log.Fatal("reqAppendEntriesReply.Success should false")
				}
				//change to follower
				rf.currentTerm = reqAppendEntriesReply.Term
				rf.state = Follower
				rf.votedFor = -1 //变成Follower重新拥有一次投票权

				rf.electTimer.Reset(RandomTimeout())
				rf.heartbeatTimer.Stop()
			} else {
				// if reqAppendEntriesReply.Success == false {
				// 	log.Fatal("reqAppendEntriesReply.Success should true")
				// }

				//心跳不算日志，不会改变logs的长度
				//...
				if reqAppendEntriesReply.Success == true {

				} else {

				}
			}
		}(i, reqAppendEntriesArgs)
	}
}

func RandomTimeout() time.Duration {
	return time.Duration(800+(rand.Int63()%150)) * time.Millisecond
}

func (rf *Raft) TimerHandle() { //被动触发
	for rf.killed() == false {
		select {
		case <-rf.electTimer.C:
			//选举超时只能发生在Follower和Candidate中
			switch rf.state { // is thread safe?
			case Follower:
				//遇到选举定时器超时，转换成Candidate，重置定时器，开始选举
				rf.StartElection(RandomTimeout())
			case Candidate:
				rf.StartElection(RandomTimeout())
			case Leader:
				fmt.Printf("Leader cannot ElectTimeOut!\n")
				rf.electTimer.Stop()
			default:
				log.Fatalf("rf.state == %v error state!\n", rf.state)
			}
		case <-rf.heartbeatTimer.C:
			if rf.state == Leader { // is thread safe?
				rf.Heartbeats()
			} else {
				log.Fatalf("rf.state == %v want to Heartbeats!\n", rf.state)
			}

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
	// go rf.ticker()
	go rf.TimerHandle()

	return rf
}
