package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	IDLE = iota
	IN_PROGRESS
	DONE
	_
	MAP
	REDUCE
	NO_JOB
)

type MapTask struct {
	filename  string
	worker_id int
	state     int
}

type ReduceTask struct {
	intermediate []KeyValue
	worker_id    int
	state        int
}

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	fmt.Println("We are in Assign Task Call")
	for _, mapTask := range(c.mapTasks) {
		if mapTask.state == IDLE {
			// reply.MTask = mapTask
			reply.TaskType = MAP
			fmt.Println("Hey")
			return nil
		}
	}

	// for _, reduceTask := range(c.reduceTasks) {
	// 	if reduceTask.state == IDLE {
	// 		reply.RTask = reduceTask
	// 		reply.TaskType = REDUCE
	// 		return nil
	// 	}
	// }

	reply.TaskType = args.Type
	return nil
}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// i think it works
	fmt.Println("We are in the Done function.")
	ret := false

	for _, tasks := range(c.reduceTasks) {
		fmt.Println("Task state is", tasks.state)
		if tasks.state != DONE {
			return false
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//i think it works
	fmt.Println("We are in the MakeCoordinator function.")
	c := Coordinator{}
	var mapTasks []MapTask
	var reduceTasks []ReduceTask

	var mapTask MapTask

	for _, filename := range files {
		mapTask.filename = filename
		mapTask.worker_id = 0
		mapTask.state = IDLE

		mapTasks = append(mapTasks, mapTask)
	}

	c.mapTasks = mapTasks
	c.reduceTasks = reduceTasks
	c.nReduce = nReduce

	c.server()
	return &c
}
