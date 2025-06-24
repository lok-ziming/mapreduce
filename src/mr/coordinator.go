package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Finished
	Timeout
	Failed
)

func (s TaskStatus) String() string {
	names := [...]string{"Idle", "Running", "Finished", "Timeout", "Failed"}
	if s < 0 || s >= TaskStatus(len(names)) {
		return "Unknown"
	}
	return names[s]
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

func (t TaskType) String() string {
	names := [...]string{"Map", "Reduce"}
	if t < 0 || t >= TaskType(len(names)) {
		return "Unknown"
	}
	return names[t]
}

type Task struct {
	Id        int
	Version   int
	DoneChan  chan TaskResult
	Files     []string
	Type      TaskType
	CreatedAt int64
	Status    TaskStatus
}

type TaskResult struct {
	Id      int
	Type    TaskType
	Version int
	Status  TaskStatus
	Result  []string
}

func newTask(id int, files []string) *Task {
	return &Task{
		Id:        id,
		Files:     files,
		CreatedAt: 0,
		Status:    Idle,
	}
}

type Coordinator struct {
	nReduce         int
	filesName       []string
	mapTasks        []*Task
	mapCh           chan *Task
	reduceTasks     []*Task
	reduceCh        chan *Task
	mainCh          chan *TaskResult
	MapTasksDone    int
	ReduceTasksDone int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	if c.MapTasksDone < len(c.mapTasks) {
		task := c.GetMapTask()
		reply.Task = *task
	} else if c.ReduceTasksDone < len(c.reduceTasks) {
		task := c.GetReduceTask()
		reply.Task = *task
	}
	reply.Task = Task{Id: -1}
	return nil
}

func (c *Coordinator) GetMapTask() *Task {
	select {
	case task := <-c.mapCh:
		task.Status = Running
		task.CreatedAt = time.Now().Unix()
		go c.TaskWithTimeout(task)
		return task
	default:
		return &Task{Id: -1}
	}
}

func (c *Coordinator) GetReduceTask() *Task {
	select {
	case task := <-c.reduceCh:
		task.Status = Running
		task.CreatedAt = time.Now().Unix()
		task.DoneChan = make(chan TaskResult, 1)
		go c.TaskWithTimeout(task)
		return task
	default:
		return &Task{Id: -1}
	}
}

func (c *Coordinator) TaskWithTimeout(task *Task) {

	select {
	case <-time.After(10 * time.Second):
		c.mainCh <- &TaskResult{
			Id:      task.Id,
			Type:    task.Type,
			Version: task.Version,
			Status:  Timeout,
		}
	case t := <-task.DoneChan:
		log.Printf("Task Done %s:%d", t.Type.String(), t.Id)
	}
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

func (c *Coordinator) TaskMonitor() {
	for task := range c.mainCh {
		var dstTask *Task
		if Map == task.Type {
			dstTask = c.mapTasks[task.Id]
		} else if Reduce == task.Type {
			dstTask = c.reduceTasks[task.Id]
		}
		if dstTask == nil {
			log.Printf("Received task with unknown ID: %d", task.Id)
			continue
		}
		if Finished == dstTask.Status || dstTask.Version != task.Version {
			log.Printf("Task result %s:%d has been Expired or Task is already finished: %v",
				task.Type.String(), task.Id, dstTask)
			continue
		}
		if Timeout == task.Status || Failed == task.Status {
			log.Printf("Task %s, retrying %s:%d", task.Status.String(), task.Type.String(), task.Id)
			dstTask.Status = Idle
			dstTask.CreatedAt = 0
			dstTask.Version++
			close(dstTask.DoneChan)
			if Map == dstTask.Type {
				c.mapCh <- dstTask
			} else if Reduce == dstTask.Type {
				c.reduceCh <- dstTask
			}
			continue
		}
		if Finished == task.Status {
			log.Printf("Task %s:%d finished successfully", task.Type.String(), task.Id)
			dstTask.Status = Finished
			close(dstTask.DoneChan)
			if Map == dstTask.Type {
				log.Printf("Map task %d finished, checking if all map tasks are done", task.Id)
			} else if Reduce == dstTask.Type {
				log.Printf("Reduce task %d finished, checking if all tasks are done", task.Id)
			}
			continue
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.MapTasksDone == len(c.mapTasks) && c.ReduceTasksDone == len(c.reduceTasks)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		filesName:   files,
		mapTasks:    make([]*Task, 0, len(files)),
		reduceTasks: make([]*Task, 0, nReduce),
		mapCh:       make(chan *Task, len(files)),
		reduceCh:    make(chan *Task, nReduce),
		mainCh:      make(chan *TaskResult, max(len(files), nReduce)),
	}
	// Your code here.
	for i, filename := range files {
		task := newTask(i, []string{filename})
		c.mapTasks = append(c.mapTasks, task)
		c.mapCh <- task
	}

	c.server()
	return &c
}
