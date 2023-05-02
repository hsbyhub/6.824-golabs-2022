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
func (c *Coordinator) OnGetMap(req *GetMapReq, rsp *GetMapRsp) error {
	c.MapTasks.Range(func(key, value any) bool {
		task, ok := value.(*MapTask)
		if !ok {
			log.Fatal("coordinator on get map convert task fail")
			return false
		}
		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status == TaskStatusNew || (task.Status == TaskStatusHandle && task.WorkExpire < time.Now().Unix()) {
			// 更新状态
			task.Status = TaskStatusHandle
			task.Worker = req.Worker
			task.WorkExpire = time.Now().Unix() + 10

			rsp.FileName = task.FileName
			rsp.BuketCount = c.BuketCount
			return false
		}
		return true
	})

	return nil
}

func (c *Coordinator) OnPostMap(req *PostMapReq, rsp *PostMapRsp) error {
	v, ok := c.MapTasks.Load(req.FileName)
	if !ok {
		return fmt.Errorf("coordinator on post map filename[%v] not found", req.FileName)
	}
	task, ok := v.(*MapTask)
	if !ok {
		return fmt.Errorf("coordinator on post map convert task fail")
	}
	task.Mu.Lock()
	defer task.Mu.Unlock()

	if req.Err != nil {
		task.Status = TaskStatusNew
		task.Worker = 0
		task.WorkExpire = 0
		return nil
	}

	// 检查
	if task.Status != TaskStatusHandle {
		return fmt.Errorf("coordinator on post map status[%v] illegal", task.Status)
	}
	if task.Worker != req.Worker {
		return fmt.Errorf("coordinator on post map worker illegal, maybe expire and distributed to other worker")
	}
	for buket, _ := range req.BuketFileNameMap {
		v, ok := c.ReduceTasks.Load(buket)
		if !ok {
			return fmt.Errorf("coordinator on post map buket reduce task not found")
		}
		reduceTask, ok := v.(*ReduceTask)
		if !ok {
			return fmt.Errorf("coordinator on post map buket reduce task convert fail")
		}
		reduceTask.Mu.Lock()
		if reduceTask.Status != TaskStatusNew {
			return fmt.Errorf("coordinator on post map buket reduce task status not new")
		}
		reduceTask.Mu.Unlock()
	}

	// 设置
	for buket, fileName := range req.BuketFileNameMap {
		v, _ := c.ReduceTasks.Load(buket)
		reduceTask := v.(*ReduceTask)
		reduceTask.Mu.Lock()
		reduceTask.FileNames = append(reduceTask.FileNames, fileName)
		reduceTask.Mu.Unlock()
	}
	task.Status = TaskStatusDone
	rsp.OK = true

	return nil
}

func (c *Coordinator) OnGetReduce(req *GetReduceReq, rsp *GetReduceRsp) error {
	if !c.isMapDone() {
		return nil
	}

	c.ReduceTasks.Range(func(key, value any) bool {
		task, ok := value.(*ReduceTask)
		if !ok {
			log.Fatal("coordinator on get reduce convert task fail")
			return true
		}

		task.Mu.Lock()
		defer task.Mu.Unlock()
		if task.Status == TaskStatusNew || (task.Status == TaskStatusHandle && task.WorkExpire < time.Now().Unix()) {
			// 更新状态
			task.Status = TaskStatusHandle
			task.Worker = req.Worker
			task.WorkExpire = time.Now().Unix() + 10

			rsp.Buket = task.Buket
			rsp.FileNames = task.FileNames
			return false
		}
		return true
	})

	return nil
}

func (c *Coordinator) OnPostReduce(req *PostReduceReq, rsp *PostReduceRsp) error {
	v, ok := c.ReduceTasks.Load(req.Buket)
	if !ok {
		return fmt.Errorf("coordinator on post reduce buket[%v] not found", req.Buket)
	}
	task, ok := v.(*ReduceTask)
	if !ok {
		return fmt.Errorf("coordinator on post reduce convert task fail")
	}
	task.Mu.Lock()
	defer task.Mu.Unlock()

	if req.Err != nil {
		task.Status = TaskStatusNew
		task.Worker = 0
		task.WorkExpire = 0
		return nil
	}

	// 检查
	if task.Status != TaskStatusHandle {
		return fmt.Errorf("coordinator on post reduce status[%v] illegal", task.Status)
	}
	if task.Worker != req.Worker {
		return fmt.Errorf("coordinator on post reduce worker illegal, maybe expire and distributed to other worker")
	}

	// 设置
	task.Status = TaskStatusDone
	rsp.OK = true

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

func (c *Coordinator) isMapDone() bool {
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

func (c *Coordinator) isReduceDone() bool {
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
	return c.isReduceDone()
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
