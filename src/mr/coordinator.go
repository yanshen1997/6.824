package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock         sync.RWMutex
	workerId     int //一个新worker注册的时候给它分配一个id，该id自增
	tempFilePath []string
	workers      map[int]*worker // id -> worker
	idleWorkers  *[]*worker
	fileMapTasks map[string]*task //通过文件路径名找到task
	idleTasks    *[]*task
	finishWg     sync.WaitGroup
	finishChan   chan struct{}
}

type worker struct {
	id         int
	state      workerState
	currTask   *task
	ticker     time.Ticker // 超时通知
	finishChan chan struct{}
	lock       sync.Mutex
}

// worker的状态应当有空闲、运转和失败
type workerState int

type task struct {
	state      taskState
	tType      taskType //map=true reduce=false
	filePath   string   // 一个任务处理一个文件
	currWorker *worker
}
type taskState int

type taskType bool

const (
	mapTask    = true
	reduceTask = false
)

const (
	idle int = iota
	inProgress
	completed
	failed
)

// Your code here -- RPC handlers for the worker to call.
// RegisterWorker 注册worker
func (c *Coordinator) RegisterWorker(args *RegArgs, reply *RegReply) error {
	defer func() {
		c.workers[c.workerId].ticker = *time.NewTicker(time.Second * 10)
		go c.monitorWorker(c.workerId)
		c.workerId++
		c.lock.Unlock()
	}()
	reply.Id = c.workerId
	//加锁避免同时多个worker注册引发竞争
	c.lock.Lock()
	c.workers[c.workerId] = &worker{
		id:         c.workerId,
		state:      workerState(idle),
		finishChan: make(chan struct{}),
	}
	idleTaskList := *c.idleTasks
	if len(idleTaskList) != 0 {
		task := idleTaskList[0]
		reply.TaskType = task.tType
		reply.FilePath = task.filePath
		*c.idleTasks = idleTaskList[1:]
		c.workers[c.workerId].state = workerState(inProgress)
		c.workers[c.workerId].currTask = task
	}
	return nil
}

// 暂时没加调度逻辑 只更新任务和worker的状态信息
func (c *Coordinator) monitorWorker(id int) {
	c.lock.RLock()
	worker := c.workers[id]
	c.lock.RUnlock()
	if worker.state != workerState(inProgress) {
		return
	}
	select { // 这里后续仔细考察一下时序。20220126
	case <-worker.ticker.C:
		// c.appandIdleTask(worker.currTask)
		c.syncChange(appandIdleTask, taskKey, worker.currTask)
		// c.appandIdleWorker(worker)
		c.syncChange(appandIdleWorker, workerKey, worker)
		worker.syncChange(taskTimeout)
	case <-worker.finishChan:
		worker.syncChange(finishTask)
		c.finishWg.Done()
	}
}

const (
	workerKey      = "worker"
	coordinatorKey = "coordinator"
	taskKey        = "task"
)

// 修改worker字段
func (w *worker) syncChange(f func(map[string]interface{}), is ...interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()
	vars := map[string]interface{}{}
	vars[workerKey] = w
	if len(is)&1 != 0 {
		fmt.Println("warn:func (w *worker) syncChange: args.length % 2 != 0")
	}
	for i := 1; i < len(is); i += 2 {
		vars[is[i-1].(string)] = is[i]
	}
	f(vars)
}

// worker完成任务后处理
func finishTask(vars map[string]interface{}) {
	w := vars[workerKey].(*worker)
	w.currTask.state = taskState(completed)
	w.currTask.currWorker = nil
	w.currTask = nil
	w.state = workerState(idle)
}

func taskTimeout(vars map[string]interface{}) {
	w := vars[workerKey].(*worker)
	curTask := w.currTask
	if curTask == nil {
		return
	}
	// 超时未完成则委派给其他worker
	curTask.state = taskState(idle)
	curTask.currWorker = nil
	w.currTask = nil
	w.state = workerState(idle)
}

func (c *Coordinator) syncChange(f func(map[string]interface{}), is ...interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	vars := make(map[string]interface{})
	vars[coordinatorKey] = c
	if len(is)&1 != 0 {
		fmt.Println("warn:func (c *Coordinator) syncChange: args.length %2 != 0")
	}
	for i := 1; i < len(is); i += 2 {
		vars[is[i-1].(string)] = is[i]
	}
	f(vars)
}

func appandIdleWorker(vars map[string]interface{}) {
	c, w := vars[coordinatorKey].(*Coordinator), vars[workerKey].(*worker)
	(*c.idleWorkers) = append((*c.idleWorkers), w)
}
func appandIdleTask(vars map[string]interface{}) {
	c, t := vars[coordinatorKey].(*Coordinator), vars[taskKey].(*task)
	(*c.idleTasks) = append((*c.idleTasks), t)
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	id := args.WorkerId
	c.lock.RLock()
	worker := c.workers[id]
	c.lock.RUnlock()
	// 下面对worker的读写不加锁是默认只会有一个worker在调用master中对应的元数据
	if worker.state == workerState(idle) {
		reply.Msg = "failed. timeout.."
		return nil
	}

	worker.finishChan <- struct{}{}
	reply.Msg = "success."
	return nil
}

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
	select {
	case <-c.finishChan:
		ret = true
	default:
	}
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
	initCoordinator(&c)
	testDemo(&c)
	c.server()
	return &c
}

// 初始化master
func initCoordinator(c *Coordinator) {
	c.lock = sync.RWMutex{}
	c.workerId = 0
	c.tempFilePath = os.Args[1:]
	c.workers = map[int]*worker{}
	c.idleWorkers = &([]*worker{})
	c.fileMapTasks = map[string]*task{}
	c.idleTasks = &([]*task{})
	c.finishWg = sync.WaitGroup{}
	c.finishChan = make(chan struct{})
}

//随机添加不少于5个、不大于25个的任务
func testDemo(c *Coordinator) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 5+rand.Intn(20); i++ {
		task := &task{
			state:    taskState(idle),
			filePath: fmt.Sprintf("test%d", i),
			tType:    mapTask,
		}
		(*c.idleTasks) = append((*c.idleTasks), task)
		c.fileMapTasks[fmt.Sprintf("test%d", i)] = task
		c.finishWg.Add(1)
	}
	fmt.Printf(" %d tasks...\n", len((*c.idleTasks)))
	go func() {
		c.finishWg.Wait()
		c.finishChan <- struct{}{}
	}()
}
