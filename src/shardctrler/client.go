package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.

	ck.clientId = nrand()
	ck.sequenceNumber = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := CommandArgs{
		OpType:         "Query",
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
		Args:           QueryArgs{num},
	}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.CommandHanler", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.sequenceNumber++
				if reply.OpType != "Query" || reply.Reply == nil {
					log.Fatalf("error type <%v>", reply)
				}

				return reply.Reply.(QueryReply).Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CommandArgs{
		OpType:         "Join",
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
		Args:           JoinArgs{servers},
	}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.CommandHanler", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.sequenceNumber++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := CommandArgs{
		OpType:         "Leave",
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
		Args:           LeaveArgs{gids},
	}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.CommandHanler", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.sequenceNumber++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CommandArgs{
		OpType:         "Move",
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
		Args: MoveArgs{
			Shard: shard,
			GID:   gid,
		},
	}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.CommandHanler", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.sequenceNumber++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
