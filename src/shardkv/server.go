package shardkv

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

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
	UpdateConfig = "UpdateConfig" //本端执行
	PullShard    = "PullShard"    //req对端执行
	InstallShard = "InstallShard" //本端执行
	EraseShard   = "EraseShard"   //req对端执行
	ServerShard  = "ServerShard"  //本端执行让shard的开始server
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
	Id           int32
	State        int32
	StateMachine KVStateMachine
}
type Command = CommandArgs

type Result struct {
	SequenceNumber int64
	CommandReply   CommandReply
}

func (kv *ShardKV) IsDuplicateRequest(clientId int64, sequenceNumber int64) bool {
	if clientId == -1 {
		return false
	}
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
	// DPrintf("begin CommandHanler() args = %v", args)
	// defer DPrintf("end CommandHanler() reply = %v", reply)
	kv.mu.Lock()
	if args.OpType == Get || args.OpType == PutAppend {
		var shard int
		switch args.OpType {
		case Get:
			shard = key2shard(args.Args.(GetArgs).Key)
		case PutAppend:
			shard = key2shard(args.Args.(PutAppendArgs).Key)
		}
		if kv.curConfig.Shards[shard] != kv.gid {
			kv.mu.Unlock()
			reply.Err = ErrWrongGroup
			reply.OpType = args.OpType
			return
		}
	}
	if args.OpType != Get && kv.IsDuplicateRequest(args.ClientId, args.SequenceNumber) {
		*reply = kv.lastResults[args.ClientId].CommandReply
		//DPrintf("kv.PutAppend,IsDuplicateRequest == true Err = %v\n", reply.Err)
		kv.mu.Unlock()
		// if args.OpType != reply.OpType {
		// 	//log.Fatalf("args<%v>but reply<%v>", args, reply)
		// 	reply.Err = Retry
		// 	reply.OpType = args.OpType
		// }
		return
	}
	kv.mu.Unlock()

	//DPrintf("sc.CommandHanler,begin start args = %v\n", args)
	index, _, isLeader := kv.rf.Start(Command(*args))
	//DPrintf("sc.CommandHanler,end start isLeader = %v\n", isLeader)
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

	// if args.OpType != reply.OpType {
	// 	//log.Fatalf("args<%v>but reply<%v>", args, reply)
	// 	reply.Err = Retry
	// 	reply.OpType = args.OpType
	// }
	return
}

func (kv *ShardKV) Applier() {
	DPrintf("entry Applier func==============\n")
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:

			if msg.CommandValid {
				kv.CommandApply(msg)
			} else if msg.SnapshotValid { //被动触发的快照
				kv.SnapshotApply(msg)
			} else {
				log.Fatalf("invalid msg<%v>\n", msg)
			}

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
	} else if cmd.OpType != Get && kv.IsDuplicateRequest(cmd.ClientId, cmd.SequenceNumber) {
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
			// kv.lastResults[cmd.ClientId] = res
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
		case InstallShard:
			args := cmd.Args.(ShardReply)
			kv.InstallShardCommand(&args)
		case EraseShard:
			kv.EraseShardCommand(&cmd, &res.CommandReply)
			res.SequenceNumber = -1
		case ServerShard:
			kv.ServerShardCommand(&cmd)
			res.SequenceNumber = -1
		default:
			log.Fatalf("not exist opType!<%v>\n", msg.Command)
		}
	}
	//_, isLeader := kv.rf.GetState()
	//明确，不是所有的命令都是由client发送的。也即不是所有命令在达成共识后，leader就要唤醒请求rpc。
	if currentTerm, isLeader := kv.rf.GetState(); isLeader == true && currentTerm == int(msg.CommandTerm) { //通知client 处理回调
		notifyChan := kv.GetNotifyChan(int64(msg.CommandIndex), false)

		if notifyChan != nil {
			notifyChan <- res
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
		if kv.shards[shard].State == Server {
			reply.Value, reply.Err = kv.shards[shard].StateMachine.Get(args.Key)
		} else { //没准备好
			reply.Err = NoReady
			if kv.shards[shard].State == Invalid {
				log.Fatalf("[gid = %v] GetCommand.NoReady state = %v \t lastCfg = %v \t curCfg = %v", kv.gid, kv.StateToString(), kv.lastConfig, kv.curConfig)
			}
		}
	} else {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) PutAppendCommand(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	if kv.curConfig.Shards[shard] == kv.gid { //是在本集群组
		if kv.shards[shard].State == Server {
			switch args.Op {
			case "Put":
				reply.Err = kv.shards[shard].StateMachine.Put(args.Key, args.Value)
			case "Append":
				reply.Err = kv.shards[shard].StateMachine.Append(args.Key, args.Value)
			default:
				log.Fatalf("in PutAppendCommand not exist opType!<%v>\n", args.Op)
			}
			DPrintf("PutAppendCommand : <%v>", args)
		} else { //没准备好
			reply.Err = NoReady
			if kv.shards[shard].State == Invalid {
				log.Fatalf("[gid = %v] GetCommand.NoReady state = %v \t lastCfg = %v \t curCfg = %v", kv.gid, kv.StateToString(), kv.lastConfig, kv.curConfig)
			}
		}
	} else {
		reply.Err = ErrWrongGroup
	}
}

// const (
//
//	Server = iota
//	Pulling
//	Wait
//	Erase
//	Invalid
//
// )
func StateToStringHelp(state int) string {
	switch state {
	case Server:
		return "Server"
	case Pulling:
		return "Pulling"
	case Wait:
		return "Wait"
	case Erase:
		return "Erase"
	case Invalid:
		return "Invalid"
	default:
		return "Unknow"
	}
}
func (kv *ShardKV) StateToString() string {
	res := ""
	for shard, db := range kv.shards {
		res += (StateToStringHelp(int(db.State)) + "[" + strconv.Itoa(shard) + "]" + ",")
	}

	return res
}
func (kv *ShardKV) UpdateConfigCommand(cfg *shardctrler.Config) {
	if cfg.Num <= kv.curConfig.Num {
		return
	}
	kv.lastConfig = kv.curConfig
	kv.curConfig = *cfg
	DPrintf("group = %v execute UpdateConfigCommand begin <%v>", kv.gid, kv.curConfig)
	if kv.lastConfig.Num == 0 {
		for shard, _ := range kv.shards {
			if kv.curConfig.Shards[shard] == kv.gid {
				kv.shards[shard].State = Server
				kv.shards[shard].StateMachine = NewMemoryKV()
			} else {
				kv.shards[shard].State = Invalid
			}
		}
		DPrintf("group = %v execute UpdateConfigCommand begin <%v>", kv.gid, kv.curConfig)
	} else {
		for shard, _ := range kv.shards {
			// 对比cfg 和 curConfig哪些该删除，哪些该pull数据
			if kv.curConfig.Shards[shard] == kv.gid {
				//新配置中该分片是该组，可能需要迁移、可能不需要
				if kv.lastConfig.Shards[shard] == kv.gid {
					//不变、无需迁移
				} else {
					// db.state == Invalid
					kv.shards[shard].State = Pulling
				}
			} else { //新配置不为该分片服务
				if kv.lastConfig.Shards[shard] == kv.gid {
					//旧配置为该分片服务
					kv.shards[shard].State = Erase
				} else {

				}
			}
		}
	}

	DPrintf("group = %v execute UpdateConfigCommand end <shard state:%v>", kv.gid, kv.StateToString())

}

func (kv *ShardKV) InstallShardCommand(sr *ShardReply) {
	if sr.ConfigNum < kv.curConfig.Num {
		return //过期的冗余命令
	}

	if kv.shards[sr.Shard].State != Pulling {
		log.Fatalf("error shard state <%v>", kv.shards[sr.Shard].State)
	}
	DPrintf("gid = %v get shard", kv.gid)
	kv.shards[sr.Shard].StateMachine = NewMemoryKV()
	kv.shards[sr.Shard].StateMachine.SetData(CopyKVMap(sr.DB.GetData()))
	kv.shards[sr.Shard].State = Wait
}

func (kv *ShardKV) EraseShardCommand(args *CommandArgs, reply *CommandReply) {
	if args.Args.(ShardArgs).ConfigNum < kv.curConfig.Num {
		// log.Fatalf("your config number is old too")
		reply.Err = Retry
		reply.OpType = args.OpType
		reply.Reply = nil
		return
	}
	if kv.shards[args.Args.(ShardArgs).Shard].State != Erase {
		DPrintf("my shards[%v] is already deleted! my state is %v", args.Args.(ShardArgs).Shard, StateToStringHelp(int(kv.shards[args.Args.(ShardArgs).Shard].State)))
		reply.Err = DumpOpt
		reply.OpType = args.OpType
		reply.Reply = nil
		return
	}
	kv.shards[args.Args.(ShardArgs).Shard].State = Invalid
	kv.shards[args.Args.(ShardArgs).Shard].StateMachine = nil

	reply.Err = OK
	reply.OpType = args.OpType
	reply.Reply = nil
}

func (kv *ShardKV) ServerShardCommand(args *CommandArgs) {
	if args.Args.(ShardArgs).ConfigNum < kv.curConfig.Num {
		log.Fatalf("args config number is old too")
	}
	if kv.shards[args.Args.(ShardArgs).Shard].State != Wait {
		log.Fatalf("my shards[%v].state is not Wait, state is %v", args.Args.(ShardArgs).Shard, StateToStringHelp(int(kv.shards[args.Args.(ShardArgs).Shard].State)))
	}
	kv.shards[args.Args.(ShardArgs).Shard].State = Server
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
	//DPrintf("kv.Applier start make snapshot, index = %v\n", kv.lastApplied)
	DPrintf("[gid = %v], in MakeSnapshot(), curCfg = %v, state = %v\n\n", kv.gid, kv.curConfig, kv.StateToString())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards) // data
	e.Encode(kv.curConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.lastResults) //客户最后一次应用记录，为了去重
	e.Encode(kv.lastApplied)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}

func (kv *ShardKV) ReadPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	var shards [shardctrler.NShards]Shard
	var curCfg shardctrler.Config
	var lastCfg shardctrler.Config
	var lastRes map[int64]Result
	var lastApplied int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shards) != nil || // bug ：不要用&kv.shards来解码！！！
		d.Decode(&curCfg) != nil ||
		d.Decode(&lastCfg) != nil ||
		d.Decode(&lastRes) != nil ||
		d.Decode(&lastApplied) != nil {
		log.Fatalf("data Decode fail!<%v>\n", r)
	} else {
		kv.shards = shards
		kv.curConfig = curCfg
		kv.lastConfig = lastCfg
		kv.lastResults = lastRes
		kv.lastApplied = lastApplied
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
	gid := kv.gid
	for shard, _ := range kv.shards {
		if kv.shards[shard].State != Server && kv.shards[shard].State != Invalid {
			needToUpdate = false
			break
		}
	}
	kv.mu.Unlock()

	if needToUpdate == true {
		cfg := kv.scck.Query( /*-1*/ curCfgNum + 1) //查询最新的配置

		if /*cfg.Num > curCfgNum*/ cfg.Num == curCfgNum+1 {
			DPrintf("need to update config <begin><%v>", cfg)
			//通过raft达成共识
			kv.rf.Start(CommandArgs{
				ClientId:       -1,
				SequenceNumber: -1,
				OpType:         UpdateConfig,
				Args:           cfg,
			})

			//to do consider to new a chan wait for consistent complete
			DPrintf("need to update config <end><%v>", cfg)
		} else {
			kv.mu.Lock()
			DPrintf("[gid = %v]stable state. the config is newst <curCfgNum = %v> curCfg = %v state = <%v>", gid, curCfgNum, kv.curConfig, kv.StateToString())
			kv.mu.Unlock()
		}
	} else {
		kv.mu.Lock()
		DPrintf("[gid = %v], temp state.. config <curCfgNum = %v> state = %v", kv.gid, curCfgNum, kv.StateToString())
		kv.mu.Unlock()
	}

}

type ShardArgs struct {
	ConfigNum int
	Shard     int
}
type ShardReply struct {
	ConfigNum int
	Shard     int
	DB        KVStateMachine
}

func CopyKVMap(originalMap map[string]string) map[string]string {
	newMap := make(map[string]string)
	for key, value := range originalMap {
		newMap[key] = value
	}
	return newMap
}
func (kv *ShardKV) PullShardHanler(args *CommandArgs, reply *CommandReply) {
	if args.OpType != PullShard {
		log.Fatalf("error OpType <%v>", args.OpType)
	}

	if _, isLeader := kv.rf.GetState(); isLeader == false {
		reply.Err = ErrWrongLeader
		reply.OpType = args.OpType
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//不能删除server状态的shard
	//如果shard所在的集群配置版本太低，则应该拒绝分片迁移。
	shard := args.Args.(ShardArgs).Shard
	if kv.curConfig.Num != args.Args.(ShardArgs).ConfigNum {
		reply.Err = NoReady
		reply.OpType = args.OpType
		return
	} else if kv.lastConfig.Shards[shard] != kv.gid {
		log.Fatalf("error PullShard")
		reply.Err = ErrWrongGroup
		reply.OpType = args.OpType
		return
	} else if kv.shards[shard].State != Erase {
		log.Fatalf("error PullShard != Erase state = %v", StateToStringHelp(int(kv.shards[shard].State)))
	}

	//分片迁移
	reply.Err = OK
	reply.OpType = args.OpType
	reply.Reply = ShardReply{
		ConfigNum: kv.curConfig.Num,
		Shard:     shard,
		DB: &MemoryKV{
			KV: CopyKVMap(kv.shards[shard].StateMachine.GetData()),
		},
	}

	return
}

// type MemoryKV struct {
// 	KV map[string]string
// }

// 分片迁移
// 1、拉取分片
func (kv *ShardKV) PullShard() {
	//make快照并调用raft达成共识。
	kv.mu.Lock()

	wg := sync.WaitGroup{}
	for shard, _ := range kv.shards {
		if kv.shards[shard].State == Pulling {

			args := &CommandArgs{
				ClientId:       -1, //注意CommandHanler需要兼容该项为-1的情况
				SequenceNumber: -1,
				OpType:         PullShard,
				Args: ShardArgs{
					ConfigNum: kv.curConfig.Num,
					Shard:     shard,
				},
			}

			wg.Add(1)
			DPrintf("[gid = %v] pull shard[%v] from [%v]", kv.gid, shard, kv.lastConfig.Shards[shard])
			//rpc拉取
			go func(args *CommandArgs) {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				// shard := key2shard(key)
				fromgid := kv.lastConfig.Shards[args.Args.(ShardArgs).Shard]
				gid := kv.gid
				if servers, ok := kv.lastConfig.Groups[fromgid]; ok {
					// try each server for the shard.
					kv.mu.Unlock()
					for si := 0; si < len(servers); si++ {
						reply := &CommandReply{}
						srv := kv.make_end(servers[si])
						ok := srv.Call("ShardKV.PullShardHanler", args, reply)
						if ok && (reply.Err == OK) {
							if reply.OpType != PullShard {
								log.Fatalf("error reply OpType %v", reply.OpType)
							}
							DPrintf("[gid = %v] pull shard[%v] from [%v]<%v>", gid, args.Args.(ShardArgs).Shard, fromgid, reply)
							kv.rf.Start(CommandArgs{
								ClientId:       -1,
								SequenceNumber: -1,
								OpType:         InstallShard,
								Args:           reply.Reply, // type is ShardReply
							})
							break
						}
						if ok && (reply.Err == ErrWrongGroup) {
							break
						}
						// ... not ok, or ErrWrongLeader
					}
					kv.mu.Lock()
				}

				wg.Done()
			}(args)
		}
	}

	kv.mu.Unlock()

	wg.Wait()
}

// 2、请求对端删除shard
func (kv *ShardKV) RequestErase() {
	//make快照并调用raft达成共识。
	//make快照并调用raft达成共识。
	kv.mu.Lock()

	wg := sync.WaitGroup{}
	for shard, _ := range kv.shards {
		if kv.shards[shard].State == Wait {
			args := &CommandArgs{
				ClientId:       -1, //注意CommandHanler需要兼容该项为-1的情况
				SequenceNumber: -1,
				OpType:         EraseShard,
				Args: ShardArgs{
					ConfigNum: kv.curConfig.Num,
					Shard:     shard,
				},
			}

			wg.Add(1)
			//rpc拉取
			go func(args *CommandArgs) {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				// shard := key2shard(key)
				gid := kv.lastConfig.Shards[args.Args.(ShardArgs).Shard]
				if servers, ok := kv.lastConfig.Groups[gid]; ok {
					kv.mu.Unlock()
					// try each server for the shard.
					for si := 0; si < len(servers); si++ {
						reply := &CommandReply{}
						srv := kv.make_end(servers[si])
						ok := srv.Call("ShardKV.CommandHanler", args, reply)
						if ok && (reply.Err == OK || reply.Err == DumpOpt) {
							if reply.OpType != EraseShard {
								log.Fatalf("error reply OpType %v", reply.OpType)
							}
							kv.rf.Start(CommandArgs{
								ClientId:       -1,
								SequenceNumber: -1,
								OpType:         ServerShard,
								Args:           args.Args,
							})
							break
						}
						if ok && (reply.Err == ErrWrongGroup /* || reply.Err == DumpOpt*/) {
							if reply.Err == DumpOpt {
								kv.mu.Lock()
								DPrintf("[gid = %v] curCfg = %v state = %v\t ", kv.gid, kv.curConfig, kv.StateToString())
								kv.mu.Unlock()
							}
							break
						}
						// ... not ok, or ErrWrongLeader
					}
					kv.mu.Lock()
				}

				wg.Done()
			}(args)
		}
	}

	kv.mu.Unlock()

	wg.Wait()
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

func DebugStateToString(shards *[shardctrler.NShards]Shard) string {
	res := ""
	for shard, db := range shards {
		res += (StateToStringHelp(int(db.State)) + "[" + strconv.Itoa(shard) + "]" + ",")
	}

	return res
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
	// labgob.Register(Op{})
	labgob.Register(CommandArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	labgob.Register(CommandReply{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetReply{})

	labgob.Register(shardctrler.Config{})

	labgob.Register(ShardArgs{})
	labgob.Register(ShardReply{})
	labgob.Register(&MemoryKV{})
	//labgob.Register(KVStateMachine{})
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
		kv.shards[i].Id = int32(i)
		kv.shards[i].State = Invalid
		kv.shards[i].StateMachine = NewMemoryKV()
	}
	//读取持久化数据

	kv.ReadPersist(persister.ReadSnapshot())

	//DPrintf("[gid = %d] state = %v \t lastCfg = %v \t curCfg = %v\n\n\n", kv.gid, kv.StateToString(), kv.lastConfig, kv.curConfig)
	//后台协程没有启动
	go kv.Applier()
	//time.Sleep(100 * time.Millisecond)
	go kv.BackGroundWork(kv.PullConfig)
	go kv.BackGroundWork(kv.PullShard)
	go kv.BackGroundWork(kv.RequestErase)

	return kv
}
