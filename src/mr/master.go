package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStat struct {
	beginTime time.Time
	fileName  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type MapTaskStat struct {
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskReduce,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*60)
}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (this *TaskStatQueue) Lock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) Unlock() {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}

func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.Lock()
	arrayLength := this.Size()
	if arrayLength == 0 {
		this.Unlock()
		return nil
	}
	ret := this.taskArray[arrayLength-1]
	this.taskArray = this.taskArray[:arrayLength-1]
	this.Unlock()
	return ret
}

func (this *TaskStatQueue) Push(taskStat TaskStatInterface) {
	this.Lock()
	if taskStat == nil {
		this.Unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskStat)
	this.Unlock()
}

func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	this.Lock()
	for taskIndex := 0; taskIndex < len(this.taskArray); {
		taskStat := this.taskArray[taskIndex]
		if (taskStat).OutOfTime() {
			outArray = append(outArray, taskStat)
			this.taskArray = append(this.taskArray[:taskIndex], this.taskArray[taskIndex+1:]...)
		} else {
			taskIndex++
		}
	}
	this.Unlock()
	return outArray
}

func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	this.Lock()
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.Unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	this.Lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	this.Unlock()
}

// ---+++---+++---+++---

type Master struct {
	// Your definitions here.

	filenames []string

	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// reduce task queue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// machine state
	isDone  bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	// check for reduce task
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		reduceTask.SetNow()
		this.reduceTaskRunning.Push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// check for map task
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		mapTask.SetNow()
		this.mapTaskRunning.Push(mapTask)
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}
	// exit
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		reply.State = TaskWait
		return nil
	}

	reply.State = TaskEnd
	this.isDone = true
	return nil
}

func (this *Master) distributeReduce() {
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   this.nReduce,
			nFiles:    len(this.filenames),
		},
	}
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		task := reduceTask
		task.partIndex = reduceIndex
		this.reduceTaskWaiting.Push(&task)
	}
}

func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// all map task Done
			// can distribute reduce tasks
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done Error")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// distribute map tasks
	mapArray := make([]TaskStatInterface, 0)
	for fIndex, fName := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName:  fName,
				fileIndex: fIndex,
				partIndex: 0,
				nReduce:   nReduce,
				nFiles:    len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	m := Master{
		mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		nReduce:        nReduce,
		filenames:      files,
	}

	// create tmp directory if not exists
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	// begin a thread to collect tasks out of time
	go m.collectOutOfTime()

	m.server()
	return &m
}

func (this *Master) collectOutOfTime() {
	for {
		time.Sleep(time.Duration(time.Second * 5))
		timeouts := this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.mapTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}
