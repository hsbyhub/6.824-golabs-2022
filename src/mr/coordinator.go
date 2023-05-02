package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskStatusNew    = 0
	TaskStatusHandle = 1
	TaskStatusDone   = 2
)

type Task struct {
	Mu         sync.Mutex
	Status     int
	Worker     int
	WorkExpire int64
}

type MapTask struct {
	Task
	FileName string
}

type ReduceTask struct {
	Task
	Buket     int
	FileNames []string
}

type Coordinator struct {
	// Your definitions here.
	BuketCount  int
	MapTasks    sync.Map
	ReduceTasks sync.Map
}

// Your code here -- RPC handlers for the worker to callCoordinator.
func (c *Coordinator) ReqMap(args *ReqMapArgs, reply *ReqMapReply) error {
	c.MapTasks.Range(func(key, value any) bool {
		task, ok := value.(*MapTask)
		if !ok {
			log.Fatal("coordinator reqmap convert task fail")
			return false
		}
		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status == TaskStatusNew || (task.Status == TaskStatusHandle && task.WorkExpire < time.Now().Unix()) {
			// 更新状态
			task.Status = TaskStatusHandle
			task.Worker = args.Worker
			task.WorkExpire = time.Now().Unix() + 10

			reply.FileName = task.FileName
			reply.BuketCount = c.BuketCount
			return false
		}
		return true
	})

	return nil
}

func (c *Coordinator) RspMap(args *RspMapArgs, reply *RspMapReply) error {
	v, ok := c.MapTasks.Load(args.FileName)
	if !ok {
		return fmt.Errorf("coordinator rspmap filename[%v] not found", args.FileName)
	}
	task, ok := v.(*MapTask)
	if !ok {
		return fmt.Errorf("coordinator rspmap convert task fail")
	}
	task.Mu.Lock()
	defer task.Mu.Unlock()

	// 检查
	if task.Status != TaskStatusHandle {
		return fmt.Errorf("coordinator rspmap status[%v] illegal", task.Status)
	}
	if task.Worker != args.Worker {
		return fmt.Errorf("coordinator rspmap worker illegal, maybe expire and distributed to other worker")
	}
	for buket, _ := range args.BuketFileNameMap {
		v, ok := c.ReduceTasks.Load(buket)
		if !ok {
			return fmt.Errorf("coordinator rspmap buket reduce task not found")
		}
		reduceTask, ok := v.(*ReduceTask)
		if !ok {
			return fmt.Errorf("coordinator rspmap buket reduce task convert fail")
		}
		reduceTask.Mu.Lock()
		if reduceTask.Status != TaskStatusNew {
			return fmt.Errorf("coordinator rspmap buket reduce task status not new")
		}
		reduceTask.Mu.Unlock()
	}

	// 设置
	for buket, fileName := range args.BuketFileNameMap {
		v, _ := c.ReduceTasks.Load(buket)
		reduceTask := v.(*ReduceTask)
		reduceTask.Mu.Lock()
		reduceTask.FileNames = append(reduceTask.FileNames, fileName)
		reduceTask.Mu.Unlock()
	}
	task.Status = TaskStatusDone

	return nil
}

func (c *Coordinator) ReqReduce(args *ReqReduceArgs, reply *ReqReduceReply) error {
	if !c.IsMapDone() {
		return fmt.Errorf("map not done")
	}
	c.ReduceTasks.Range(func(key, value any) bool {
		task, ok := value.(*ReduceTask)
		if !ok {
			log.Fatal("coordinator reqreduce convert task fail")
			return true
		}

		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status == TaskStatusNew || (task.Status == TaskStatusHandle && task.WorkExpire < time.Now().Unix()) {
			// 更新状态
			task.Status = TaskStatusHandle
			task.Worker = args.Worker
			task.WorkExpire = time.Now().Unix() + 10

			reply.Buket = task.Buket
			reply.FileNames = task.FileNames
			return false
		}
		return true
	})

	return nil
}

func (c *Coordinator) RspReduce(args *RspReduceArgs, reply *RspReduceReply) error {
	v, ok := c.ReduceTasks.Load(args.Buket)
	if !ok {
		return fmt.Errorf("coordinator rspreduce buket[%v] not found", args.Buket)
	}
	task, ok := v.(*ReduceTask)
	if !ok {
		return fmt.Errorf("coordinator rspreduce convert task fail")
	}
	task.Mu.Lock()
	defer task.Mu.Unlock()

	// 检查
	if task.Status != TaskStatusHandle {
		return fmt.Errorf("coordinator rspreduce status[%v] illegal", task.Status)
	}
	if task.Worker != args.Worker {
		return fmt.Errorf("coordinator rspreduce worker illegal, maybe expire and distributed to other worker")
	}

	// 设置
	task.Status = TaskStatusDone

	return nil
}

func (c *Coordinator) IsDone(args *IsDoneArgs, reply *IsDoneReply) error {
	reply.IsDone = c.Done()
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

func (c *Coordinator) IsMapDone() bool {
	ret := true
	c.MapTasks.Range(func(key, value any) bool {
		task, ok := value.(*MapTask)
		if !ok {
			log.Fatal("coordinator reqreduce convert task fail")
			return false
		}

		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status != TaskStatusDone {
			ret = false
			return false
		}
		return true
	})
	return ret
}

func (c *Coordinator) IsReduceDone() bool {
	ret := true
	c.ReduceTasks.Range(func(key, value any) bool {
		task, ok := value.(*ReduceTask)
		if !ok {
			log.Fatal("coordinator reqreduce convert task fail")
			return false
		}

		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status != TaskStatusDone {
			ret = false
			return false
		}
		return true
	})
	return ret
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.IsReduceDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		BuketCount: nReduce,
	}

	// Your code here.
	for _, fileName := range files {
		task := &MapTask{
			Task: Task{
				Status: TaskStatusNew,
			},
			FileName: fileName,
		}
		c.MapTasks.Store(fileName, task)
	}

	for buket := 0; buket < c.BuketCount; buket++ {
		task := &ReduceTask{
			Task: Task{
				Status: TaskStatusNew,
			},
			Buket:     buket,
			FileNames: make([]string, 0),
		}
		c.ReduceTasks.Store(buket, task)
	}

	c.server()
	return &c
}
