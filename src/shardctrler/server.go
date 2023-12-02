package shardctrler

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num //kvraft里面类似于KVStateMachine

	dead         int32
	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	lastResults  map[int64]Result
	notifyChan   map[int64]chan Result
}

type Command = CommandArgs

type Result struct {
	SequenceNumber int64
	CommandReply   CommandReply
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) IsDuplicateRequest(clientId int64, sequenceNumber int64) bool {
	if res, ok := sc.lastResults[clientId]; ok && res.SequenceNumber >= sequenceNumber {
		return true
	}
	return false
}

func (sc *ShardCtrler) GetNotifyChan(commandIndex int64, autoCreate bool) chan Result {
	var ch chan Result = nil
	var ok bool
	if ch, ok = sc.notifyChan[commandIndex]; ok == true {
		//找到返回
		return ch
	}
	ch = nil
	if autoCreate == true {
		ch = make(chan Result, 1)
		sc.notifyChan[commandIndex] = ch
	}

	return ch
}

func (sc *ShardCtrler) RemoveNotifyChan(commandIndex int64) {
	if _, ok := sc.notifyChan[commandIndex]; ok == true {
		//找到，删除返回
		delete(sc.notifyChan, commandIndex)
	}
}

func (sc *ShardCtrler) CommandHanler(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	//判断操作是否冗余，冗余字节返回，否则，应用到状态机
	DPrintf("begin CommandHanler()")
	defer DPrintf("end CommandHanler()")
	sc.mu.Lock()
	if sc.IsDuplicateRequest(args.ClientId, args.SequenceNumber) {
		*reply = sc.lastResults[args.ClientId].CommandReply
		//DPrintf("kv.PutAppend,IsDuplicateRequest == true Err = %v\n", reply.Err)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	DPrintf("sc.CommandHanler,begin start args = %v\n", args)
	index, _, isLeader := sc.rf.Start(Command(*args))
	DPrintf("sc.CommandHanler,end start isLeader = %v\n", isLeader)
	if isLeader != true {
		//不是leader
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		reply.OpType = args.OpType
		return
	}

	var ch chan Result = nil
	sc.mu.Lock()
	ch = sc.GetNotifyChan(int64(index), true)
	sc.mu.Unlock()

	select {
	case res := <-ch:
		*reply = res.CommandReply
	case <-time.After(time.Millisecond * 500): // 超时返回
		reply.WrongLeader = false
		reply.Err = ErrTimeout
		reply.OpType = args.OpType
	}
	//DPrintf("kv.PutAppend, unblock\n")
	// go func() { //一定要使用协程吗？能不能就在函数中处理？
	// 	kv.mu.Lock()
	// 	kv.RemoveNotifyChan(int64(index))
	// 	kv.mu.Unlock()
	// }()
	sc.mu.Lock()
	sc.RemoveNotifyChan(int64(index))
	sc.mu.Unlock()
	return
}

func MapCopy(data map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range data {
		res[k] = make([]string, len(v))
		copy(res[k], v)
	}

	return res
}

func GIdToShards(cfg Config) map[int][]int {
	res := make(map[int][]int)
	for gid, _ := range cfg.Groups {
		if gid == 0 {
			log.Fatalf("invalid gid == 0")
		}
		res[gid] = make([]int, 0)
	}

	for shard, gid := range cfg.Shards {
		if gid == 0 {
			DPrintf("shard -> gid == 0!")
		}
		res[gid] = append(res[gid], shard)
	}
	return res
}

func MaximumGroup(gIdToShards map[int][]int) int {
	resGId := -1
	for gid, shards := range gIdToShards {
		if gid == 0 { // gid为零的不作数
			continue
		}
		if resGId == -1 ||
			len(shards) > len(gIdToShards[resGId]) ||
			len(shards) == len(gIdToShards[resGId]) && gid > resGId {
			resGId = gid
		}
	}
	// if resGId == -1 {
	// 	DPrintf("MaximumGroup.gIdToShards = %v", gIdToShards)
	// }
	return resGId
}
func MinmumGroup(gIdToShards map[int][]int) int {
	resGId := -1
	for gid, shards := range gIdToShards {
		if gid == 0 { // gid为零的不作数
			continue
		}
		if resGId == -1 ||
			len(shards) < len(gIdToShards[resGId]) ||
			len(shards) == len(gIdToShards[resGId]) && gid < resGId {
			resGId = gid
		}
	}
	// if resGId == -1 {
	// 	DPrintf("MinmumGroup.gIdToShards = %v", gIdToShards)
	// }
	return resGId
}
func AdjustToBalance(gIdToShards map[int][]int) {

	if _, ok := gIdToShards[0]; ok == true {
		shards := gIdToShards[0]

		for _, shard := range shards {
			minGId := MinmumGroup(gIdToShards)
			if minGId == -1 { //gIdToShards中，除0外，没有更小的group id
				continue
			}
			gIdToShards[minGId] = append(gIdToShards[minGId], shard)
		}
		delete(gIdToShards, 0)
	}

	for {
		min, max := MinmumGroup(gIdToShards), MaximumGroup(gIdToShards)
		if min == -1 || max == -1 { //gIdToShards中，除0外，没有更小的group id
			//一般是，存在唯一的无效group id：0
			break
		}
		if len(gIdToShards[max])-len(gIdToShards[min]) > 1 { //最大的补一个给最小
			mvShard := gIdToShards[max][len(gIdToShards[max])-1]
			gIdToShards[max] = gIdToShards[max][:len(gIdToShards[max])-1]
			gIdToShards[min] = append(gIdToShards[min], mvShard)
		} else { //足够平衡
			break
		}
	}
}

/*
将args传来的gid->servers的映射，追加到config中，
尽可能减少分片迁移的前提下，保持负载均衡
*/
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	lastCfg := &sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    len(sc.configs),
		Shards: lastCfg.Shards,
		Groups: MapCopy(lastCfg.Groups),
	}

	//添加，存在的覆盖
	for gid, srvs := range args.Servers {
		newCfg.Groups[gid] = srvs
	}
	gTos := GIdToShards(newCfg)

	AdjustToBalance(gTos) //负载均衡

	for gid, shards := range gTos {
		for _, shard := range shards {
			newCfg.Shards[shard] = gid
		}
	}

	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	lastCfg := &sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    len(sc.configs),
		Shards: lastCfg.Shards,
		Groups: MapCopy(lastCfg.Groups),
	}

	//删除
	for _, dgid := range args.GIDs {
		if _, ok := newCfg.Groups[dgid]; ok == false {
			continue
		}

		delete(newCfg.Groups, dgid)
		for shard, gid := range newCfg.Shards {
			if gid == dgid {
				newCfg.Shards[shard] = 0
			}
		}
	}
	gTos := GIdToShards(newCfg)

	AdjustToBalance(gTos) //负载均衡

	for gid, shards := range gTos {
		for _, shard := range shards {
			newCfg.Shards[shard] = gid
		}
	}

	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	reply.WrongLeader = false

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	lastCfg := &sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    len(sc.configs),
		Shards: lastCfg.Shards,
		Groups: MapCopy(lastCfg.Groups),
	}

	//更改组
	newCfg.Shards[args.Shard] = args.GID
	gTos := GIdToShards(newCfg)

	AdjustToBalance(gTos) //负载均衡

	for gid, shards := range gTos {
		for _, shard := range shards {
			newCfg.Shards[shard] = gid
		}
	}

	sc.configs = append(sc.configs, newCfg)
	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	num := args.Num

	if num < 0 || num >= len(sc.configs) { //超过范围
		num = len(sc.configs) - 1
	}

	cfg := &sc.configs[num]

	reply.Config = Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: MapCopy(cfg.Groups),
	}

	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Applier() {
	DPrintf("entry Applier func==============\n")
	for sc.killed() == false {
		select {
		case msg := <-sc.applyCh:
			DPrintf("begin =================%v\n", msg)
			if msg.CommandValid {
				sc.CommandApply(msg)
			} else if msg.SnapshotValid { //被动触发的快照
				sc.SnapshotApply(msg)
			} else {
				log.Fatalf("invalid msg<%v>\n", msg)
			}

			DPrintf("end =================%v\n", msg)
		}
	}

	DPrintf("end Applier func==============\n")
}

func (sc *ShardCtrler) CommandApply(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var res Result
	cmd := msg.Command.(Command)
	//DPrintf("kv.Applier, command = %v\n", cmd)
	if msg.CommandIndex <= sc.lastApplied { //command index 要按顺序递增
		// log.Fatalf("except command index, want to get %v, but got %v", kv.lastApplied+1, msg.CommandIndex)
		DPrintf("except command index, want to get %v, but got %v", sc.lastApplied+1, msg.CommandIndex)
		return
	} else if sc.IsDuplicateRequest(cmd.ClientId, cmd.SequenceNumber) {
		//命令重复
		res = sc.lastResults[cmd.ClientId]
	} else {
		switch cmd.OpType { // 防止sequence number 回溯！
		case "Join":
			args := cmd.Args.(JoinArgs)
			reply := JoinReply{}
			sc.Join(&args, &reply)
			res.CommandReply = CommandReply{
				WrongLeader: reply.WrongLeader,
				Err:         reply.Err,
				OpType:      cmd.OpType,
				Reply:       reply,
			}
		case "Leave":
			args := cmd.Args.(LeaveArgs)
			reply := LeaveReply{}
			sc.Leave(&args, &reply)
			res.CommandReply = CommandReply{
				WrongLeader: reply.WrongLeader,
				Err:         reply.Err,
				OpType:      cmd.OpType,
				Reply:       reply,
			}
		case "Move":
			args := cmd.Args.(MoveArgs)
			reply := MoveReply{}
			sc.Move(&args, &reply)
			res.CommandReply = CommandReply{
				WrongLeader: reply.WrongLeader,
				Err:         reply.Err,
				OpType:      cmd.OpType,
				Reply:       reply,
			}
		case "Query":
			args := cmd.Args.(QueryArgs)
			reply := QueryReply{}
			sc.Query(&args, &reply)
			res.CommandReply = CommandReply{
				WrongLeader: reply.WrongLeader,
				Err:         reply.Err,
				OpType:      cmd.OpType,
				Reply:       reply,
			}
		default:
			log.Fatalf("not exist opType!<%v>\n", msg.Command)
		}

		res.SequenceNumber = cmd.SequenceNumber
		sc.lastResults[cmd.ClientId] = res
	}
	//_, isLeader := kv.rf.GetState()
	if currentTerm, isLeader := sc.rf.GetState(); isLeader == true && currentTerm == int(msg.CommandTerm) { //通知client 处理回调
		notifyChan := sc.GetNotifyChan(int64(msg.CommandIndex), false)

		if notifyChan != nil {
			DPrintf("notifyChan <- res begin")
			notifyChan <- res
			DPrintf("notifyChan <- res end")
		}
	}
	sc.lastApplied = msg.CommandIndex
	if sc.maxraftstate != -1 && (sc.rf.RaftStateSize() >= sc.maxraftstate) {
		//太大，需要压缩
		sc.MakeSnapshot()
	}

	return
}

func (sc *ShardCtrler) SnapshotApply(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if msg.SnapshotIndex <= sc.lastApplied {
		return
	}
	sc.ReadPersist(msg.Snapshot)

	return
}

func (sc *ShardCtrler) MakeSnapshot() {
	DPrintf("kv.Applier start make snapshot, index = %v\n", sc.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)     // data
	e.Encode(sc.lastResults) //客户最后一次应用记实录，为了去重
	e.Encode(sc.lastApplied)

	sc.rf.Snapshot(sc.lastApplied, w.Bytes())
}

func (sc *ShardCtrler) ReadPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(sc.rf.ReadSnapshot())
	d := labgob.NewDecoder(r)

	var data []Config
	var lastResults map[int64]Result
	var lastApply int
	if d.Decode(&data) != nil ||
		d.Decode(&lastResults) != nil ||
		d.Decode(&lastApply) != nil {
		log.Fatalf("data Decode fail!<%v>\n", r)
	} else {
		sc.configs = data
		sc.lastResults = lastResults
		sc.lastApplied = lastApply
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.maxraftstate = -1
	sc.lastApplied = 0
	sc.lastResults = make(map[int64]Result)
	sc.notifyChan = make(map[int64]chan Result)

	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	labgob.Register(JoinReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryReply{})
	go sc.Applier()
	return sc
}

// configs []Config // indexed by config num //kvraft里面类似于KVStateMachine

// dead         int32
// maxraftstate int // snapshot if log grows this big
// lastApplied  int
// lastResults  map[int64]Result
// notifyChan   map[int64]chan Result
