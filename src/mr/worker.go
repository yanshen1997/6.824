package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var (
	thisWorker  *worker
	thisMapf    *func(string, string) []KeyValue
	thisReducef *func(string, []string) string
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	CallRegister(&mapf, &reducef)
	// runTask(mapf, reducef, thisWorker.currTask)
	// CallFinish()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallRegister(mapf *func(string, string) []KeyValue,
	reducef *func(string, []string) string) {
	args := RegArgs{}
	reply := RegReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		os.Exit(1)
	}
	thisWorker = &worker{
		id: reply.Id,
		currTask: &task{
			tType:    reply.TaskType,
			filePath: reply.FilePath,
		},
		state: workerState(inProgress),
	}
	thisMapf, thisReducef = mapf, reducef
	runTask(*thisMapf, *thisReducef, thisWorker.currTask)
}

func CallFinish() {
	args := FinishArgs{
		WorkerId: thisWorker.id,
	}
	reply := FinishReply{}
	ok := call("Coordinator.Finish", &args, &reply)
	if !ok {
		fmt.Printf("call Finish failed!\n")
	}
	fmt.Println(reply.Msg)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func runTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, t *task) {
	rand.Seed(time.Now().UnixNano())
	timeSecond := rand.Intn(10)
	fmt.Printf("task running...\ncurrTask:%+v\nwill exec %d s\n", *t, timeSecond)
	time.Sleep(time.Duration(timeSecond) * time.Second)
	CallFinish()
}
