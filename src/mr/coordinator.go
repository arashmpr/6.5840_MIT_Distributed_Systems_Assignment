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
)

type MapTask struct {
	wid			int
	filename 	string
	state		int
}

type ReduceTask struct {
	wid		int
	key		string
	state	int
}

type WorkerSpec struct {
	wid			int
	taskType	int
	state		int
	taskInfo	string	// if taskType == MAP -> taskInfo = filename else taskInfo = key
}

type Coordinator struct {
	wid_counter 		int
	nReduce				int
	mts					[]MapTask
	rts					[]ReduceTask
	workers				[]WorkerSpec
	intermediate_locs	[]string
	output_loc			string

	mapStatus			bool
	isDone				bool		
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c* Coordinator) GetWorkerID(req *RPCRequest, res *RPCResponse) error {
	c.wid_counter ++
	res.WorkerID = c.wid_counter
	return nil
}

func (c* Coordinator) GetMapTask(req *RPCRequest, res *RPCResponse) error {
	for _, mt := range(c.mts) {
		if mt.state == IDLE {
			res.TaskType = MAP
			res.TaskInfo = mt.filename
			break
		}
	}
	return nil
}

func (c* Coordinator) GetReduceTask(req *RPCRequest, res *RPCResponse) error {
	// requires more work
	// not finished not tested
	for _, rt := range(c.rts) {
		if rt.state == IDLE {
			res.TaskType = REDUCE
			res.TaskInfo = rt.key
			break
		}
	}
	return nil
}

func (c* Coordinator) CheckMapStatus(req *RPCRequest, res *RPCResponse) error {
	for _, mt := range(c.mts) {
		if mt.state == IDLE {
			res.MapStatus = IDLE
			return nil
		}
		if mt.state == IN_PROGRESS {
			res.MapStatus = IN_PROGRESS
		}
	}
	if res.MapStatus == IN_PROGRESS {
		return nil
	}
	res.MapStatus = DONE
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
	// requires more work
	fmt.Println("Check if master work is done...")
	ret := false

	for _, rt := range(c.rts) {
		if rt.state != DONE {
			return false
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//requires more work
	fmt.Println("We are in the MakeCoordinator function.")
	c := Coordinator{}
	var mts []MapTask
	var rts []ReduceTask

	var mt MapTask

	for _, filename := range files {
		mt.filename = filename
		mt.wid = -1
		mt.state = IDLE

		mts = append(mts, mt)
	}

	c.wid_counter = 0
	c.nReduce = nReduce
	c.mts = mts
	c.rts = rts
	
	

	c.server()
	return &c
}
