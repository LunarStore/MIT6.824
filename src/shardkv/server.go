package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	scck        *shardctrler.Clerk
	lastApplied int
	lastResults map[int64]Result      //key：ClientId，value：result
	notifyChan  map[int64]chan Result //key：commandIndex，value：result
	dead        int32

	//shard
	lastConfig shardctrler.Config
	curConfig  shardctrler.Config
	shards     [shardctrler.NShards]Shard //定长数值，拷贝会自动深拷贝、而变长的·切片数组则是浅拷贝。
}

// OpType
const (
	Get          = "Get"
	PutAppend    = "PutAppend"
	UpdateConfig = "UpdateConfig"
)

/*
Server		//分片可以读写
Pulling		//分片正在从其他group拉取
Wait(Stop)	//分片等待从被拉取的group中删除（保持原子性）
Erase		//分片等待被拉取后，删除
Invalid		//分片无效
*/
const (
	Server = iota
	Pulling
	Wait
	Erase
	Invalid
)

type Shard struct {
	id           int32
	state        int32
	stateMachine KVStateMachine
}
type Command = CommandArgs

type Result struct {
	SequenceNumber int64
	CommandReply   CommandReply
}

func (kv *ShardKV) IsDuplicateRequest(clientId int64, sequenceNumber int64) bool {
	if res, ok := kv.lastResults[clientId]; ok && res.SequenceNumber >= sequenceNumber {
		return true
	}
	return false
}

func (kv *ShardKV) GetNotifyChan(commandIndex int64, autoCreate bool) chan Result {
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

func (kv *ShardKV) RemoveNotifyChan(commandIndex int64) {
	if _, ok := kv.notifyChan[commandIndex]; ok == true {
		//找到，删除返回
		delete(kv.notifyChan, commandIndex)
	}
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) CommandHanler(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	//判断操作是否冗余，冗余字节返回，否则，应用到状态机
	DPrintf("begin CommandHanler()")
	defer DPrintf("end CommandHanler()")
	kv.mu.Lock()
	if kv.IsDuplicateRequest(args.ClientId, args.SequenceNumber) {
		*reply = kv.lastResults[args.ClientId].CommandReply
		//DPrintf("kv.PutAppend,IsDuplicateRequest == true Err = %v\n", reply.Err)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("sc.CommandHanler,begin start args = %v\n", args)
	index, _, isLeader := kv.rf.Start(Command(*args))
	DPrintf("sc.CommandHanler,end start isLeader = %v\n", isLeader)
	if isLeader != true {
		//不是leader
		reply.Err = ErrWrongLeader
		reply.OpType = args.OpType
		return
	}

	var ch chan Result = nil
	kv.mu.Lock()
	ch = kv.GetNotifyChan(int64(index), true)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		*reply = res.CommandReply
	case <-time.After(time.Millisecond * 500): // 超时返回
		reply.Err = ErrTimeout
		reply.OpType = args.OpType
	}
	kv.mu.Lock()
	kv.RemoveNotifyChan(int64(index))
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) Applier() {
	DPrintf("entry Applier func==============\n")
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			DPrintf("begin =================%v\n", msg)
			if msg.CommandValid {
				kv.CommandApply(msg)
			} else if msg.SnapshotValid { //被动触发的快照
				kv.SnapshotApply(msg)
			} else {
				log.Fatalf("invalid msg<%v>\n", msg)
			}

			DPrintf("end =================%v\n", msg)
		}
	}

	DPrintf("end Applier func==============\n")
}

func (kv *ShardKV) CommandApply(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var res Result
	cmd := msg.Command.(Command)
	//DPrintf("kv.Applier, command = %v\n", cmd)
	if msg.CommandIndex <= kv.lastApplied { //command index 要按顺序递增
		// log.Fatalf("except command index, want to get %v, but got %v", kv.lastApplied+1, msg.CommandIndex)
		DPrintf("except command index, want to get %v, but got %v", kv.lastApplied+1, msg.CommandIndex)
		return
	} else if kv.IsDuplicateRequest(cmd.ClientId, cmd.SequenceNumber) {
		//命令重复
		res = kv.lastResults[cmd.ClientId]
	} else {
		//对命令进行处理
		switch cmd.OpType {
		case Get:
			args := cmd.Args.(GetArgs)
			reply := GetReply{}
			kv.GetCommand(&args, &reply)
			res.CommandReply = CommandReply{
				Err:    reply.Err,
				OpType: cmd.OpType,
				Reply:  reply,
			}

			res.SequenceNumber = cmd.SequenceNumber
			kv.lastResults[cmd.ClientId] = res
		case PutAppend:
			args := cmd.Args.(PutAppendArgs)
			reply := PutAppendReply{}
			kv.PutAppendCommand(&args, &reply)
			res.CommandReply = CommandReply{
				Err:    reply.Err,
				OpType: cmd.OpType,
				Reply:  reply,
			}

			res.SequenceNumber = cmd.SequenceNumber
			kv.lastResults[cmd.ClientId] = res
		case UpdateConfig:
			args := cmd.Args.(shardctrler.Config)
			kv.UpdateConfigCommand(&args)
		default:
			log.Fatalf("not exist opType!<%v>\n", msg.Command)
		}
	}
	//_, isLeader := kv.rf.GetState()
	//明确，不是所有的命令都是由client发送的。也即不是所有命令在达成共识后，leader就要唤醒请求rpc。
	if currentTerm, isLeader := kv.rf.GetState(); isLeader == true && currentTerm == int(msg.CommandTerm) { //通知client 处理回调
		notifyChan := kv.GetNotifyChan(int64(msg.CommandIndex), false)

		if notifyChan != nil {
			DPrintf("notifyChan <- res begin")
			notifyChan <- res
			DPrintf("notifyChan <- res end")
		}
	}
	kv.lastApplied = msg.CommandIndex
	if kv.maxraftstate != -1 && (kv.rf.RaftStateSize() >= kv.maxraftstate) {
		//太大，需要压缩
		kv.MakeSnapshot()
	}

	return
}
func (kv *ShardKV) GetCommand(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	if kv.curConfig.Shards[shard] == kv.gid { //是在本集群组
		if kv.shards[shard].state == Server {
			reply.Value, reply.Err = kv.shards[shard].stateMachine.Get(args.Key)
		} else { //没准备好
			reply.Err = NoReady
		}
	} else {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) PutAppendCommand(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	if kv.curConfig.Shards[shard] == kv.gid { //是在本集群组
		if kv.shards[shard].state == Server {
			switch args.Op {
			case "Put":
				reply.Err = kv.shards[shard].stateMachine.Put(args.Key, args.Value)
			case "Append":
				reply.Err = kv.shards[shard].stateMachine.Append(args.Key, args.Value)
			default:
				log.Fatalf("in PutAppendCommand not exist opType!<%v>\n", args.Op)
			}

		} else { //没准备好
			reply.Err = NoReady
		}
	} else {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) UpdateConfigCommand(cfg *shardctrler.Config) {
	if cfg.Num <= kv.curConfig.Num {
		return
	}
	kv.lastConfig = kv.curConfig
	kv.curConfig = *cfg
	for shard, db := range kv.shards {
		// 对比cfg 和 curConfig哪些该删除，哪些该pull数据
		if kv.curConfig.Shards[shard] == kv.gid {
			//新配置中该分片是该组，可能需要迁移、可能不需要
			if kv.lastConfig.Shards[shard] == kv.gid {
				//不变、无需迁移
			} else {
				// db.state == Invalid
				db.state = Pulling
			}
		} else { //新配置不为该分片服务
			if kv.lastConfig.Shards[shard] == kv.gid {
				//旧配置为该分片服务
				db.state = Erase
			} else {

			}
		}
	}
}

func (kv *ShardKV) SnapshotApply(msg raft.ApplyMsg) {
	//应用leader发来的快照
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}
	kv.ReadPersist(msg.Snapshot)

	return
}

func (kv *ShardKV) MakeSnapshot() {
	//make快照并调用raft达成共识。
	DPrintf("kv.Applier start make snapshot, index = %v\n", kv.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards) // data
	e.Encode(kv.curConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.lastResults) //客户最后一次应用记实录，为了去重
	e.Encode(kv.lastApplied)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}

func (kv *ShardKV) ReadPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.shards) != nil ||
		d.Decode(&kv.curConfig) != nil ||
		d.Decode(&kv.lastConfig) != nil ||
		d.Decode(&kv.lastResults) != nil ||
		d.Decode(&kv.lastApplied) != nil {
		log.Fatalf("data Decode fail!<%v>\n", r)
	} else {
	}
}

func (kv *ShardKV) BackGroundWork(task func()) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			task()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// 配置拉取协程
func (kv *ShardKV) PullConfig() {
	//make快照并调用raft达成共识。
	needToUpdate := true
	kv.mu.Lock()
	curCfgNum := kv.curConfig.Num
	for _, db := range kv.shards {
		if db.state != Server && db.state != Invalid {
			needToUpdate = false
			break
		}
	}
	kv.mu.Unlock()

	if needToUpdate == true {
		cfg := kv.scck.Query(-1) //查询最新的配置

		if cfg.Num > curCfgNum {
			//通过raft达成共识
			kv.rf.Start(CommandArgs{
				ClientId:       -1,
				SequenceNumber: -1,
				OpType:         UpdateConfig,
				Args:           cfg,
			})
		}
	}

}

// 分片迁移
// 1、拉取分片
func (kv *ShardKV) PullShard() {
	//make快照并调用raft达成共识。

}

// 2、请求对端删除shard
func (kv *ShardKV) RequestErase() {
	//make快照并调用raft达成共识。

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	//kv.stateMachine = NewMemoryKV()
	kv.lastApplied = 0
	kv.lastResults = make(map[int64]Result)
	kv.notifyChan = make(map[int64]chan Result)
	kv.dead = 0
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.scck = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//shards 没有初始化
	kv.curConfig.Groups = make(map[int][]string)
	kv.lastConfig.Groups = make(map[int][]string)
	for i, _ := range kv.shards {
		kv.shards[i].id = int32(i)
		kv.shards[i].state = Invalid
		kv.shards[i].stateMachine = NewMemoryKV()
	}
	//读取持久化数据

	kv.ReadPersist(persister.ReadSnapshot())
	//后台协程没有启动
	return kv
}
