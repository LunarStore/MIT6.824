package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu             sync.Mutex
	leaderId       int32
	clientId       int64
	sequenceNumber int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.sequenceNumber = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{Key: key,
		ClientId:       ck.clientId, //自从user调用
		SequenceNumber: ck.sequenceNumber,
	}

	for {
		reply := GetReply{}
		DPrintf("ck.Get, key = %v sequence number =%v\n", key, args.SequenceNumber)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout { // reply.Err == "ErrWrongLeader " || reply.Err == "ErrTimeout"
			//命令没有执行成功，其commandId保持原值，直至命令执行成功，才可增加commandId
			ck.leaderId = (ck.leaderId + 1) % (int32(len(ck.servers))) //换一个leader再试
			continue
		}

		ck.sequenceNumber++
		return reply.Value
	}

	//存在或者，不存在
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{Key: key,
		Value:          value,
		Op:             op,
		ClientId:       ck.clientId, //自从user调用
		SequenceNumber: ck.sequenceNumber,
	}

	for {
		reply := PutAppendReply{}
		DPrintf("ck.PutAppend,key = %v value = %v op = %v ClientId = %v sequence number = %v\n", args.Key,
			args.Value,
			args.Op,
			args.ClientId,
			args.SequenceNumber)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout { // reply.Err == "ErrWrongLeader " || reply.Err == "ErrTimeout "
			//命令没有执行成功，其commandId保持原值，直至命令执行成功，才可增加commandId
			ck.leaderId = (ck.leaderId + 1) % (int32(len(ck.servers))) //换一个leader再试
			continue
		}

		ck.sequenceNumber++
		break
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, optPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, optAppend)
}
