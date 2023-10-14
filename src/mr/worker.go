package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task := GetTask()
		if task == nil {
			return
		}
		// fmt.Printf("%v\n", *task)
		switch task.state {
		case Mapping:
			MappingTask(task, mapf)
		case Reducing:
			ReducingTask(task, reducef)
		case Wait:
			time.Sleep(1 * time.Second) //还有map没做完，等一会再请求任务
		case Complete: //所有任务都完成
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func GetTask() *Task {
	args := MethodArgs{Active: GT}

	reply := GetTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
	return &Task{
		state:     reply.State,
		path:      reply.Path,
		id:        reply.Id,
		nReduce:   reply.NReduce,
		timeStamp: time.Duration(reply.MagicNumber),
	}
}

func PostTask(task *Task, result []string) error {
	args := MethodArgs{}

	args.Active = PT
	args.State = task.state
	args.Path = result
	args.Id = task.id
	args.NReduce = task.nReduce
	args.MagicNumber = int64(task.timeStamp)

	reply := PostTaskReply{}

	ok := call("Coordinator.SubmitTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}

	return reply.Err
}

// func CloseAll(files []*os.File) {
// 	for _, file := range files {
// 		file.Close()
// 	}
// }

// enc := json.NewEncoder(file)
//
//	for _, kv := ... {
//		err := enc.Encode(&kv)
func MappingTask(task *Task, mapf func(string, string) []KeyValue) {

	// intermediate := make([][]KeyValue, task.nReduce)
	intermediate := make([]*json.Encoder, task.nReduce)
	files := []*os.File{}
	result := []string{}
	// enc := json.NewEncoder(file)
	for index, _ := range intermediate { //创建json文件
		filename := fmt.Sprintf("mr-%v-%v", task.id, index)
		// file, err := os.Create(filename)
		dir, _ := os.Getwd()
		tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		result = append(result, filename)
		files = append(files, tempFile)
		if err != nil {
			log.Fatalf("cannot create temp file\n")
		}
		intermediate[index] = json.NewEncoder(tempFile)
	}

	// defer CloseAll(files)
	for _, filename := range task.path {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		contents, err := ioutil.ReadAll(file)

		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		file.Close()

		kvas := mapf(filename, string(contents))

		for _, kv := range kvas {
			// kva = append(kva, kv)

			err := intermediate[ihash(kv.Key)%task.nReduce].Encode(kv)

			if err != nil {
				log.Fatalf("json encode error: %v", err)
			}
		}
	}

	for i, tempf := range files {
		tempf.Close()
		os.Rename(tempf.Name(), result[i])
	}
	//to do post task
	err := PostTask(task, result)

	if err != nil {
		log.Fatalf("post task error:%v", err)
	}
}

// dec := json.NewDecoder(file)
//
//	for {
//		var kv KeyValue
//		if err := dec.Decode(&kv); err != nil {
//			break
//		}
//		kva = append(kva, kv)
//	}
func ReducingTask(task *Task, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%v", task.id)
	dir, _ := os.Getwd()
	tempFile, _ := ioutil.TempFile(dir, "mr-tmp-*")
	intermediate := []KeyValue{}

	for _, filename := range task.path {
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), oname)
	//to do post task
	err := PostTask(task, []string{oname})

	if err != nil {
		log.Fatalf("post task error:%v", err)
	}
}
