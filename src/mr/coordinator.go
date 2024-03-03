package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
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
	wid      int
	filename string
	state    int
	}

type ReduceTask struct {
	wid      	int
	filename 	string
	state    	int
	no			int
	}

type WorkerSpec struct {
	wid      int
	state    int
}

type Coordinator struct {
	wid_counter int
	nReduce     int
	mts         []MapTask
	rts         []ReduceTask
	workers     []WorkerSpec

	map_status    int
	reduce_status int
	is_done       bool
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Handshake(req *RPCRequest, res *RPCResponse) error {
	new_worker := WorkerSpec{}
	new_worker.wid = c.wid_counter
	new_worker.state = IDLE
	c.workers = append(c.workers, new_worker)

	res.WorkerID = c.wid_counter

	c.wid_counter++
	return nil
}

func (c *Coordinator) GetTask(req *RPCRequest, res *RPCResponse) error {
	c.UpdateMapStatus()
	for c.map_status != DONE {
		switch c.map_status {
		case IN_PROGRESS:
			time.Sleep(10 * time.Millisecond)
			c.UpdateMapStatus()
		case IDLE:
			for i, mt := range c.mts {
				if mt.state == IDLE {
					c.mts[i].wid = req.WorkerID
					c.mts[i].state = IN_PROGRESS

					res.TaskType = MAP
					res.TaskInfo = mt.filename
					return nil
				}
			}
		}
	}

	c.UpdateReduceStatus()
	for c.reduce_status != DONE {
		switch c.reduce_status {
		case IN_PROGRESS:
			time.Sleep(10 * time.Millisecond)
			c.UpdateReduceStatus()
		case IDLE:
			for i, rt := range c.rts {
				if rt.state == IDLE {
					c.rts[i].wid = req.WorkerID
					c.rts[i].state = IN_PROGRESS

					res.TaskType = REDUCE
					res.TaskInfo = rt.filename
					res.ReduceNo = rt.no
					return nil
				}
			}
		}
	}

	res.TaskType = NO_JOB
	c.is_done = true
	return nil
}

func (c *Coordinator) UpdateMapStatus() {
	map_status := -1
	for _, mt := range c.mts {
		if mt.state == IDLE {
			map_status = IDLE
			c.map_status = map_status
			return
		}
		if mt.state == IN_PROGRESS {
			map_status = IN_PROGRESS
		}
	}
	if map_status == -1 {
		map_status = DONE
	} else {
		map_status = IN_PROGRESS
	}
	c.map_status = map_status
}

func (c *Coordinator) PrintStatus() {
	fmt.Println()
	fmt.Println()

	fmt.Println("MAP STATUS")
	fmt.Println("----------------------------------------")
	fmt.Println("idx\twid\tstate")
	for idx, mt := range c.mts {
		fmt.Printf("%d\t%d\t%d\n", idx, mt.wid, mt.state)
	}

	fmt.Println()
	fmt.Println()

	fmt.Println("REDUCE STATUS")
	fmt.Println("----------------------------------------")
	fmt.Println("idx\twid\tstate")
	for idx, rt := range c.rts {
		fmt.Printf("%d\t%d\t%d\n", idx, rt.wid, rt.state)
	}
}

func (c *Coordinator) UpdateReduceStatus() {
	reduce_status := -1
	for _, rt := range c.rts {
		if rt.state == IDLE {
			c.reduce_status = IDLE
			return
		}
		if rt.state == IN_PROGRESS {
			reduce_status = IN_PROGRESS
		}
	}
	if reduce_status == -1 {
		reduce_status = DONE
	} else {
		reduce_status = IN_PROGRESS
	}
	c.reduce_status = reduce_status
}

func (c *Coordinator) SetMapStatus(req *RPCRequest, res *RPCResponse) error {
	wid := req.WorkerID
	mt_status := req.MapStatus

	for i, mt := range c.mts {
		if mt.wid == wid {
			c.mts[i].state = mt_status
		}
	}
	return nil
}

func (c *Coordinator) SetReduceStatus(req *RPCRequest, res *RPCResponse) error {
	wid := req.WorkerID
	rt_status := req.ReduceStatus

	for i, rt := range c.rts {
		if rt.wid == wid {
			c.rts[i].state = rt_status
		}
	}
	return nil
}

func (c *Coordinator) FreeWorker(req *RPCRequest, res *RPCResponse) error {
	wid := res.WorkerID
	for _, worker := range c.workers {
		if wid == worker.wid {
			worker.wid = IDLE
		}
	}
	return nil
}

func (c *Coordinator) WriteToIntermediatePaths(intermediatePaths [][]KeyValue) error {
	for idx, _ := range intermediatePaths {
		if len(intermediatePaths[idx]) == 0 {
			continue
		}
		intFilename := "intermediate_" + strconv.Itoa(idx) + ".json"

		file, err := os.OpenFile(intFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Coordinator::WriteToIntermediatePaths: Error opening file:", err)
			return err
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range intermediatePaths[idx] {
			enc.Encode(&kv)
		}
	}
	return nil
}

func (c *Coordinator) DistributeIntermedite(req *RPCRequest, res *RPCResponse) error {
	intermediate := req.Intermediate

	var intermediate_paths [][]KeyValue = make([][]KeyValue, c.nReduce)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % c.nReduce
		intermediate_paths[idx] = append(intermediate_paths[idx], kv)
	}
	c.WriteToIntermediatePaths(intermediate_paths)
	return nil
}

func (c *Coordinator) CreateMapTasks(files []string) []MapTask {
	var mts []MapTask

	var mt MapTask

	for _, filename := range files {
		mt.filename = filename
		mt.wid = -1
		mt.state = IDLE

		mts = append(mts, mt)
	}

	return mts
}

func (c *Coordinator) CreateReduceTasks(nReduce int) []ReduceTask {
	var rts []ReduceTask
	for i := 0; i < nReduce; i++ {
		rt := ReduceTask{}
		rt.wid = -1
		rt.filename = "intermediate_" + strconv.Itoa(i) + ".json"
		rt.state = IDLE
		rt.no = i
		file, _ := os.Create(rt.filename)
		file.Close()
		rts = append(rts, rt)
	}
	return rts
}

func (c *Coordinator) isWokerAlive() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("Signal the worker")
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.is_done {
		return true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	mts := c.CreateMapTasks(files)
	rts := c.CreateReduceTasks(nReduce)

	c.wid_counter = 0
	c.nReduce = nReduce
	c.mts = mts
	c.rts = rts

	c.map_status = IDLE
	c.reduce_status = IDLE
	c.is_done = false

	c.server()
	go c.isWokerAlive()

	return &c
}
