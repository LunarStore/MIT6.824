package mr

import (
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	Map = iota
	Mapping
	Wait
	Reduce
	Reducing
	Complete
)

// 定时器和任务结合在一起
type Task struct {
	state   uint     //任务状态
	path    []string //输入/输出文件的路径
	id      int      //任务编号
	nReduce int

	timeStamp time.Duration //超时时间
	timeNode  *list.Element /* *list.Element*/ //方便删除 //必须用uintptr否则用对象指针会递归的构造rpc返回对象！！！
}

type Coordinator struct {
	// Your definitions here.
	mapTask      map[int]*Task
	mappingTask  map[int]*Task
	reduceTask   map[int]*Task
	reducingTask map[int]*Task
	completeTask map[int]*Task

	nReduce int //reduce最大任务号

	GTCh chan CallArg
	PTCh chan CallArg

	//定时器
	timerManager list.List //to do 修改成map	//插入排序
	timer        *time.Timer
	cCount       int32 //完成任务数
}

type CallArg struct {
	args  interface{}
	reply interface{}
	wait  chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *MethodArgs, reply *GetTaskReply) error {
	// fmt.Printf("Welcom :%v!\n", args.Active)
	ca := CallArg{args: args, reply: reply, wait: make(chan struct{}, 2)}
	c.GTCh <- ca
	<-ca.wait //等待schedule处理

	return nil
}

func (c *Coordinator) SubmitTask(args *MethodArgs, reply *PostTaskReply) error {
	// fmt.Printf("Welcom :%v!\n", args.Active)
	ca := CallArg{args: args, reply: reply, wait: make(chan struct{}, 2)}
	c.PTCh <- ca
	<-ca.wait //等待schedule处理
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
func (c *Coordinator) Scheduler() {
	for {
		// fmt.Printf("loop...")
		select {

		case ca := <-c.GTCh:
			// fmt.Printf("take map \n")
			if len(c.mapTask) != 0 { //还有map任务
				var task *Task = nil
				for _, v := range c.mapTask {
					//不能边遍历边删除
					// delete(c.mapTask, k) //任务状态改变成mapping，从map表中移除
					task = v
					v.timeStamp = time.Duration(time.Now().UnixNano()) + 10*time.Second
					v.state = Mapping
					c.AddTimer(v) //10秒定时

					*ca.reply.(*GetTaskReply) = GetTaskReply{
						State:       v.state,
						Path:        v.path,
						Id:          v.id,
						NReduce:     v.nReduce,
						MagicNumber: int64(v.timeStamp),
					}

					c.mappingTask[v.id] = v //移到mapping表中
					break
				}
				if task != nil {
					delete(c.mapTask, task.id)
				}

			} else if len(c.mappingTask) != 0 { //没有map任务但还有mapp任务没完成，等待其他map完成，发生超时，由你接手
				//wait任务
				reply := ca.reply.(*GetTaskReply)
				reply.State = Wait
				reply.Id = -1 //wait任务不需要回复
				reply.NReduce = c.nReduce
				reply.Path = nil      //负责等待就行了
				reply.MagicNumber = 0 //不存在即可
				//end
			} else if len(c.reduceTask) != 0 { //map阶段完成，还有reduce任务没分配完
				//reduce任务
				var task *Task = nil
				for _, v := range c.reduceTask {
					//delete(c.reduceTask, k) //任务状态改变成reducing，从reduce表中移除
					task = v
					v.timeStamp = time.Duration(time.Now().UnixNano()) + 10*time.Second
					v.state = Reducing
					c.AddTimer(v) //10秒定时

					*ca.reply.(*GetTaskReply) = GetTaskReply{
						State:       v.state,
						Path:        v.path,
						Id:          v.id,
						NReduce:     v.nReduce,
						MagicNumber: int64(v.timeStamp),
					}

					c.reducingTask[v.id] = v //移到reducing表中
					break
				}
				if task != nil {
					delete(c.reduceTask, task.id)
				}
			} else if len(c.reducingTask) != 0 { //reduce任务分配完，再请求的worker等待，任务超时由你接手
				//wait任务
				reply := ca.reply.(*GetTaskReply)
				reply.State = Wait
				reply.Id = -1 //wait任务不需要回复
				reply.NReduce = c.nReduce
				reply.Path = nil //负责等待就行了
				reply.MagicNumber = 0
				//end
			} else {
				if len(c.completeTask) != c.nReduce {
					log.Fatalf("nReduce output file's number is error:%v\n", len(c.completeTask))
				}
				//complete任务
				reply := ca.reply.(*GetTaskReply)
				reply.State = Complete
				reply.Id = -1 //Complete任务不需要回复
				reply.NReduce = c.nReduce
				reply.Path = nil //负责等待就行了
				reply.NReduce = 0
				atomic.AddInt32(&c.cCount, 1)
			}
			ca.wait <- struct{}{}
		case ca := <-c.PTCh:
			// fmt.Printf("put task \n")
			args := ca.args.(*MethodArgs)
			if args.State == Mapping { //转化成reduce状态
				//判断一下是不是旧的超时的Mappingtask
				if c.mappingTask[args.Id] == nil ||
					args.MagicNumber != int64(c.mappingTask[args.Id].timeStamp) {
					//旧的残留的任务
					ca.reply.(*PostTaskReply).Err = nil
					ca.wait <- struct{}{}
					break
				}
				c.RemoveTimer(c.mappingTask[args.Id]) //map任务完成，从定时器队列中移除
				delete(c.mappingTask, args.Id)

				//分发文件
				for _, fn := range args.Path {
					index := strings.LastIndexByte(fn, '-')

					if index == -1 {
						log.Fatalf("filename error")
					}

					num, err := strconv.Atoi(fn[index+1:])
					if err != nil || num >= c.nReduce || num < 0 {
						fmt.Printf("转换失败:%v, filename:%v\n", err, fn)
						// ca.reply.(*PostTaskReply).err = PostError{"文件名转化失败"}
						continue
					}

					if c.reduceTask[num] == nil {
						c.reduceTask[num] = &Task{
							state:   Reduce,
							id:      num,
							nReduce: c.nReduce,
						}
					}

					c.reduceTask[num].path = append(c.reduceTask[num].path, fn)
				}

				ca.reply.(*PostTaskReply).Err = nil
			} else if args.State == Reducing { //转化成complete状态
				//判断一下是不是旧的超时的Reducingtask
				if c.reducingTask[args.Id] == nil ||
					args.MagicNumber != int64(c.reducingTask[args.Id].timeStamp) {
					//旧的残留的任务
					ca.reply.(*PostTaskReply).Err = nil
					ca.wait <- struct{}{}
					break
				}
				task := c.reducingTask[args.Id]
				c.RemoveTimer(task) //reduce任务完成，从定时器队列中移除
				delete(c.reducingTask, args.Id)

				task.state = Complete
				task.path = args.Path
				c.completeTask[args.Id] = task

				ca.reply.(*PostTaskReply).Err = nil
			} else { //不可能存在其他状态的任务提交
				log.Fatalf("post task' state never reach!!!")
			}
			ca.wait <- struct{}{}
		case <-c.timer.C: //处理超时
			fmt.Printf("time out \n")
			tasks := c.ListExpire(time.Duration(time.Now().UnixNano()))
			//只有reduce任务和map任务有定时

			for _, ts := range tasks {
				switch ts.state {
				case Mapping:
					delete(c.mappingTask, ts.id)
					ts.state = Map
					c.mapTask[ts.id] = ts
				case Reducing:
					delete(c.reducingTask, ts.id)
					ts.state = Reduce
					c.reduceTask[ts.id] = ts
				default:
					log.Fatalf("任务状态有误！")
				}
			}

			if c.timerManager.Len() == 0 {
				c.timer.Stop()
			} else {
				c.timer.Reset(c.timerManager.Front().Value.(*Task).timeStamp - time.Duration(time.Now().UnixNano()))
			}
		}

	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool { //1s

	// Your code here.

	return int(atomic.LoadInt32(&c.cCount)) == c.nReduce //any to int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	gob.Register(Task{})
	c.mapTask = make(map[int]*Task)
	c.mappingTask = make(map[int]*Task)
	c.reduceTask = make(map[int]*Task)
	c.reducingTask = make(map[int]*Task)
	c.completeTask = make(map[int]*Task)

	for i, fn := range files {
		c.mapTask[i] = &Task{
			state:     Map,
			path:      []string{fn},
			id:        i,
			nReduce:   nReduce,
			timeStamp: 0, //任务被分配时初始化
		}
	}

	c.nReduce = nReduce
	c.timerManager.Init()      //初始化双向链表
	c.timer = time.NewTimer(0) //初始化timer
	//不可启动
	c.timer.Stop()
	atomic.StoreInt32(&c.cCount, 0)

	c.GTCh = make(chan CallArg, 10)
	c.PTCh = make(chan CallArg, 10)
	go c.Scheduler() //启动任务调度协程
	c.server()
	return &c
}

//定时器实现

// 插入定时器
func (c *Coordinator) AddTimer(task *Task) { //如果刚好插入到队头，需要重置定时器
	cur := c.timerManager.Front()
	for ; cur != nil; cur = cur.Next() {
		if cur.Value.(*Task).timeStamp > task.timeStamp { //找到第一个比task大的
			break
		}
	}

	if c.timerManager.Front() == nil ||
		c.timerManager.Front().Value.(*Task).timeStamp > task.timeStamp {
		c.timer.Reset(task.timeStamp - time.Duration(time.Now().UnixNano())) //超时更新
	}
	if cur != nil {
		task.timeNode = c.timerManager.InsertBefore(task, cur)
	} else {
		task.timeNode = c.timerManager.PushBack(task)
	}
}

//删除定时器

func (c *Coordinator) RemoveTimer(task *Task) {
	needToReset := false
	if task.timeNode == c.timerManager.Front() {
		needToReset = true
	}
	c.timerManager.Remove(task.timeNode)

	if c.timerManager.Len() == 0 {
		c.timer.Stop()
	} else if needToReset {
		c.timer.Reset(c.timerManager.Front().Value.(*Task).timeStamp - time.Duration(time.Now().UnixNano()))
	}
}

func (c *Coordinator) ListExpire(now time.Duration) []*Task {
	rt := []*Task{}
	cur := c.timerManager.Front()
	for cur != nil {
		next := cur.Next()
		if cur.Value.(*Task).timeStamp > now {
			break
		}

		rt = append(rt, cur.Value.(*Task))
		c.timerManager.Remove(cur)

		cur = next
	}

	return rt
}
