package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock         sync.Mutex
	workerId     int //一个新worker注册的时候给它分配一个id，该id自增
	tempFilePath []string
	workers      map[int]worker
	task         map[string]task //通过文件路径名找到task
}

type worker struct {
	id       int
	state    workerState
	currTask *task
	ticker   time.Ticker // 超时通知
}

// worker的状态应当有空闲、运转和失败
type workerState int

type task struct {
	state      taskState
	tType      bool   //map=true reduce=false
	filePath   string // 一个任务处理一个文件
	currWorker *worker
}
type taskState int

const (
	idle int = iota
	inProgress
	completed
	failed
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
