package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapState = 0
	ReduceState = 1
	FinishState =2
)
type Coordinator struct {
	// Your definitions here.
	Inputs []string
	State int
	MapTaskNum int
	ReduceTaskNum int
	TaskQueue []Task
	PendingTasks map[string]Task
	Lock sync.RWMutex
}

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

func (c *Coordinator)checkFailedTask(){
	for {
		c.Lock.Lock()
		if c.State == FinishState{
			c.Lock.Unlock()
			return
		}
		rmKeys := []string{}
		for key,task := range c.PendingTasks{
			duration := time.Now().Sub(task.StartTime)
			if duration > time.Second*10{
				rmKeys = append(rmKeys,key)
			}
		}
		for _,key := range rmKeys{
			c.TaskQueue = append(c.TaskQueue,c.PendingTasks[key])
			delete(c.PendingTasks,key)
		}
		c.Lock.Unlock()
		time.Sleep(time.Second*5)
	}
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
	c.Lock.Lock()
	defer c.Lock.Unlock()
	ret := c.State == FinishState

	return ret
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs,reply *RequestTaskReturn)error  {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if c.State == MapState{
		if len(c.TaskQueue) == 0{
			reply.Task = &Task{
				SLEEP,
				0,
				nil,
				time.Now(),
			}
		} else{
			task :=c.TaskQueue[0]
			task.StartTime = time.Now()
			c.TaskQueue = c.TaskQueue[1:]
			reply.Task = &task
			reply.NReduce = c.ReduceTaskNum
			c.PendingTasks[fmt.Sprintf("%d&%d",reply.TaskType,reply.TaskNumber)] = task
			//fmt.Println("xxxxxx",reply.Task.TaskType,reply.Task.TaskNumber,reply.Task.Input,reply.NReduce)
			//
		}
	} else if c.State == ReduceState{
		if len(c.TaskQueue) == 0{
			reply.Task = &Task{
				SLEEP,
				0,
				nil,
				time.Now(),
			}
		}else{
			task :=c.TaskQueue[0]
			task.StartTime = time.Now()
			c.TaskQueue = c.TaskQueue[1:]
			reply.Task = &task
			reply.NReduce = c.ReduceTaskNum
			c.PendingTasks[fmt.Sprintf("%d&%d",task.TaskType,task.TaskNumber)] = task
		}
	} else{
		reply.Task = &Task{
			SLEEP,
			0,
			nil,
			time.Now(),
		}
	}
	return nil
}

func  (c *Coordinator) ReportTask(args *ReportTaskArgs,reply *ReportTaskReturn)error{
	taskKey := fmt.Sprintf("%d&%d",args.Task.TaskType,args.Task.TaskNumber)
	c.Lock.Lock()
	defer c.Lock.Unlock()
	task := c.PendingTasks[taskKey]
	delete(c.PendingTasks,taskKey)
	if task.TaskType == MAP{
		c.Inputs = append(c.Inputs,args.Output...)
		if len(c.TaskQueue) == 0 && len(c.PendingTasks) == 0{
			for i:=0;i < c.ReduceTaskNum;i++{
				c.TaskQueue = append(c.TaskQueue,Task{
					REDUCE,
					i,
					[]string{},
					time.Now(),
				})
			}
			for _,file := range c.Inputs{
				Y,_ := strconv.Atoi(file[len(file)-1:])
				c.TaskQueue[Y].Input = append(c.TaskQueue[Y].Input,file)
			}
			c.State = ReduceState
		}
	} else if task.TaskType == REDUCE{
		if len(c.TaskQueue) == 0 && len(c.PendingTasks) == 0{
			c.State = FinishState
		}
	}
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		[]string{},
		MapState,
		len(files),
		nReduce,
		[]Task{},
		make(map[string]Task),
		sync.RWMutex{},
	}
	for idx,f := range files{
		c.TaskQueue =append(c.TaskQueue,Task{
			MAP,
			idx,
			[]string{f},
			time.Now(),
		})
	}
	go c.checkFailedTask()
	c.server()
	return &c
}
