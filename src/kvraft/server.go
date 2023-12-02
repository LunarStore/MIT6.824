package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

const (
	optGet    = "Get"
	optPut    = "Put"
	optAppend = "Append"
)

/*
opType == Get : key字段有效，value字段无效
opType == Put/Appen : key/value字段均有效。
*/
type Command struct {
	OpType         string
	Key            string
	Value          string
	ClientId       int64
	SequenceNumber int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine KVStateMachine
	lastApplied  int
	lastResults  map[int64]Result      //key：ClientId，value：result
	notifyChan   map[int64]chan Result //key：commandIndex，value：result

	// delta int // log delta
}

type Result struct {
	SequenceNumber int64
	State          Err
	Value          string
}

//内存kv状态机
/*######################################*/
type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
	Length() int
	GetData() map[string]string
	SetData(data map[string]string)
	Clear()
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok { //不存在的key，不会因为查询而自动被创建
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (memoryKV *MemoryKV) Length() int {
	return len(memoryKV.KV)
}

func (memoryKV *MemoryKV) GetData() map[string]string {
	return memoryKV.KV
}
func (memoryKV *MemoryKV) SetData(data map[string]string) {
	memoryKV.KV = data
}

func (memoryKV *MemoryKV) Clear() {
	memoryKV.KV = make(map[string]string)
}

/*######################################*/

func (kv *KVServer) IsDuplicateRequest(clientId int64, sequenceNumber int64) bool {
	if res, ok := kv.lastResults[clientId]; ok && res.SequenceNumber >= sequenceNumber {
		return true
	}
	return false
}

func (kv *KVServer) GetNotifyChan(commandIndex int64, autoCreate bool) chan Result {
	var ch chan Result = nil
	var ok bool
	if ch, ok = kv.notifyChan[commandIndex]; ok == true {
		//找到返回
		return ch
	}
	ch = nil
	if autoCreate == true {
		ch = make(chan Result, 1)
		kv.notifyChan[commandIndex] = ch
	}

	return ch
}

func (kv *KVServer) RemoveNotifyChan(commandIndex int64) {
	if _, ok := kv.notifyChan[commandIndex]; ok == true {
		//找到，删除返回
		delete(kv.notifyChan, commandIndex)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//判断操作是否冗余，冗余字节返回，否则，应用到状态机
	DPrintf("begin Get()")
	defer DPrintf("end Get()")
	kv.mu.Lock()
	if kv.IsDuplicateRequest(args.ClientId, args.SequenceNumber) {
		reply.Value, reply.Err = kv.lastResults[args.ClientId].Value, kv.lastResults[args.ClientId].State
		//DPrintf("kv.Get,IsDuplicateRequest == true Err = %v\n", reply.Err)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("kv.Get, begin start")
	index, _, isLeader := kv.rf.Start(Command{
		OpType:         optGet,
		Key:            args.Key,
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	})
	DPrintf("kv.Get, end start")
	if isLeader != true {
		//不是leader
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	var ch chan Result = nil
	kv.mu.Lock()
	ch = kv.GetNotifyChan(int64(index), true)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.State
	case <-time.After(time.Millisecond * 500): // 超时返回
		reply.Value, reply.Err = "", ErrTimeout
	}

	// go func() {
	// 	kv.mu.Lock()
	// 	kv.RemoveNotifyChan(int64(index))
	// 	kv.mu.Unlock()
	// }()
	kv.mu.Lock()
	kv.RemoveNotifyChan(int64(index))
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//判断操作是否冗余，冗余字节返回，否则，应用到状态机
	//判断操作是否冗余，冗余字节返回，否则，应用到状态机
	DPrintf("begin PutAppend()")
	defer DPrintf("end PutAppend()")
	kv.mu.Lock()
	if kv.IsDuplicateRequest(args.ClientId, args.SequenceNumber) {
		reply.Err = kv.lastResults[args.ClientId].State
		//DPrintf("kv.PutAppend,IsDuplicateRequest == true Err = %v\n", reply.Err)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("kv.PutAppend,begin start args = %v\n", args)
	index, _, isLeader := kv.rf.Start(Command{
		OpType:         args.Op,
		Key:            args.Key,
		Value:          args.Value,
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	})
	DPrintf("kv.PutAppend,end start isLeader = %v\n", isLeader)
	if isLeader != true {
		//不是leader
		reply.Err = ErrWrongLeader
		return
	}

	var ch chan Result = nil
	kv.mu.Lock()
	ch = kv.GetNotifyChan(int64(index), true)
	kv.mu.Unlock()

	//DPrintf("kv.PutAppend, block\n")
	select {
	case res := <-ch:
		reply.Err = res.State
	case <-time.After(time.Millisecond * 500): // 超时返回
		reply.Err = ErrTimeout
	}
	//DPrintf("kv.PutAppend, unblock\n")
	// go func() { //一定要使用协程吗？能不能就在函数中处理？
	// 	kv.mu.Lock()
	// 	kv.RemoveNotifyChan(int64(index))
	// 	kv.mu.Unlock()
	// }()
	kv.mu.Lock()
	kv.RemoveNotifyChan(int64(index))
	kv.mu.Unlock()
	return
}

func (kv *KVServer) Applier() {
	DPrintf("entry Applier func==============\n")
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			DPrintf("begin =================%v\n", msg)
			//将entry应用到状态机中。
			kv.mu.Lock()
			if msg.CommandValid {
				var res Result
				cmd := msg.Command.(Command)
				//DPrintf("kv.Applier, command = %v\n", cmd)
				if msg.CommandIndex <= kv.lastApplied { //command index 要按顺序递增
					// log.Fatalf("except command index, want to get %v, but got %v", kv.lastApplied+1, msg.CommandIndex)
					DPrintf("except command index, want to get %v, but got %v", kv.lastApplied+1, msg.CommandIndex)
					kv.mu.Unlock()
					continue
				} else if kv.IsDuplicateRequest(cmd.ClientId, cmd.SequenceNumber) {
					//命令重复
					res = kv.lastResults[cmd.ClientId]
					// kv.mu.Unlock()
					// continue
				} else {
					switch cmd.OpType { // 防止sequence number 回溯！
					case optGet:
						res.Value, res.State = kv.stateMachine.Get(cmd.Key)
						res.SequenceNumber = cmd.SequenceNumber
					case optPut:
						res.State = kv.stateMachine.Put(cmd.Key, cmd.Value)
						res.SequenceNumber = cmd.SequenceNumber
					case optAppend:
						//曾追加应用到状态机的逻辑。
						res.State = kv.stateMachine.Append(cmd.Key, cmd.Value)
						res.SequenceNumber = cmd.SequenceNumber
					default:
						log.Fatalf("not exist opType!<%v>\n", msg.Command)
					}

					kv.lastResults[cmd.ClientId] = res
				}

				notifyChan := kv.GetNotifyChan(int64(msg.CommandIndex), false)
				//_, isLeader := kv.rf.GetState()
				if currentTerm, isLeader := kv.rf.GetState(); notifyChan != nil && isLeader == true && currentTerm == int(msg.CommandTerm) { //通知client 处理回调
					DPrintf("notifyChan <- res begin")
					notifyChan <- res
					DPrintf("notifyChan <- res end")
				}
				// if notifyChan != nil { //通知client 处理回调
				// 	DPrintf("notifyChan <- res begin")
				// 	notifyChan <- res
				// 	DPrintf("notifyChan <- res end")
				// }
				// kv.delta += msg.CommandIndex - kv.lastApplied
				kv.lastApplied = msg.CommandIndex
				//DPrintf("kv.lastApplied = %v, log real len = %v log len = %v\n", kv.lastApplied, kv.rf.GetRealLength(), kv.rf.GetLength())
				if kv.maxraftstate != -1 && (kv.rf.RaftStateSize() >= kv.maxraftstate) {
					DPrintf("kv.Applier start make snapshot, index = %v\n", kv.lastApplied)
					// kv.delta = 0
					//太大，需要压缩
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.stateMachine.GetData())
					e.Encode(kv.lastResults) //客户最后一次应用记实录，为了去重
					e.Encode(kv.lastApplied)

					kv.rf.Snapshot(kv.lastApplied, w.Bytes())

					// kv.stateMachine.Clear() //清空即可

				}
			} else if msg.SnapshotValid { //被动触发的快照
				DPrintf("---------")
				if msg.SnapshotIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				// kv.rf.Snapshot(msg.SnapshotIndex, msg.Snapshot)
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)

				var data map[string]string
				var lr map[int64]Result
				var la int
				if d.Decode(&data) != nil ||
					d.Decode(&lr) != nil ||
					d.Decode(&la) != nil {
					log.Fatalf("data Decode fail!<%v>\n", msg.Snapshot)
				} else {
					kv.stateMachine.SetData(data)
					kv.lastResults = lr
					kv.lastApplied = la
				}
			} else {
				log.Fatalf("invalid msg<%v>\n", msg)
			}

			kv.mu.Unlock()
			DPrintf("end =================%v\n", msg)
		}
	}

	DPrintf("end Applier func==============\n")
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(kv.rf.ReadSnapshot())
	d := labgob.NewDecoder(r)

	var data map[string]string
	var lr map[int64]Result
	var la int
	if d.Decode(&data) != nil ||
		d.Decode(&lr) != nil ||
		d.Decode(&la) != nil {
		log.Fatalf("data Decode fail!<%v>\n", r)
	} else {
		kv.stateMachine.SetData(data)
		kv.lastResults = lr
		kv.lastApplied = la
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0
	// You may need initialization code here.
	kv.stateMachine = NewMemoryKV()
	kv.lastApplied = 0
	kv.lastResults = make(map[int64]Result)
	kv.notifyChan = make(map[int64]chan Result)
	// kv.delta = 0

	kv.readPersist(kv.rf.ReadSnapshot())
	go kv.Applier()
	return kv
}
