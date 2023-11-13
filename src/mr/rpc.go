package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	GT = iota //get task
	PT        //post task
)

type MethodArgs struct {
	Active uint
	//result
	State   uint     //任务状态
	Path    []string //输入/输出文件的路径
	Id      int      //任务编号
	NReduce int

	MagicNumber int64 //for检测task有没有过期
}

type GetTaskReply struct {
	State   uint     //任务状态
	Path    []string //输入/输出文件的路径
	Id      int      //任务编号
	NReduce int

	MagicNumber int64 //for检测task有没有过期
}

type PostTaskReply struct {
	Err error
}

type PostError struct {
	info string
}

func (pe *PostError) Error() string {
	return pe.info
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
