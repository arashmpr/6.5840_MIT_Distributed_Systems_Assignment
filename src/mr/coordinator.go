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
	wid      int
	filename string
	state    int
}

type ReduceTask struct {
	wid   int
	key   string
	state int
}

type WorkerSpec struct {
	wid      int
	taskType int
	state    int
	taskInfo string // if taskType == MAP -> taskInfo = filename else taskInfo = key
}

type Coordinator struct {
	wid_counter int
	nReduce     int
	mts         []MapTask
	rts         []ReduceTask
	workers     []WorkerSpec
	intfiles    []string
	outfile     string

	mapStatus bool
	isDone    bool
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

func (c *Coordinator) GetWorkerID(req *RPCRequest, res *RPCResponse) error {
	c.wid_counter++
	res.WorkerID = c.wid_counter
	return nil
}

func (c *Coordinator) GetMapTask(req *RPCRequest, res *RPCResponse) error {
	for _, mt := range c.mts {
		if mt.state == IDLE {
			res.TaskType = MAP
			res.TaskInfo = mt.filename
			break
		}
	}
	return nil
}

func (c *Coordinator) GetReduceTask(req *RPCRequest, res *RPCResponse) error {
	// requires more work
	// not finished not tested
	for _, rt := range c.rts {
		if rt.state == IDLE {
			res.TaskType = REDUCE
			res.TaskInfo = rt.key
			break
		}
	}
	return nil
}

func (c *Coordinator) CheckMapStatus(req *RPCRequest, res *RPCResponse) error {
	for _, mt := range c.mts {
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

func (c *Coordinator) WriteToIntermediatePaths(intermediate_paths [][]KeyValue) error {
	for idx, _ := range intermediate_paths {
		if len(intermediate_paths[idx]) == 0 {
			continue
		}
		intfilename := "intermediate_" + strconv.Itoa(idx)
		data, err := json.Marshal(intermediate_paths[idx])
		if err != nil {
			fmt.Println("Coordinator::WriteToIntermediatePaths: Error serializing data:", err)
			return err
		}
		err = os.WriteFile(intfilename, data, 0644)
		if err != nil {
			fmt.Println("Coordinator::WriteToIntermediatePaths: Error writing file:", err)
			return err
		}

		fmt.Printf("Data written successfully to '%s'\n", intfilename)
	}
	fmt.Println("All data written succcessfully")
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

func (c *Coordinator) CreateIntermediateFiles(nReduce int) []string {
	var intfiles []string
	for i := 0; i < nReduce; i++ {
		intfile := "intermediate_" + strconv.Itoa(i)
		file, _ := os.Create(intfile)
		file.Close()
		intfiles = append(intfiles, intfile)
	}
	return intfiles
}

func (c *Coordinator) CreateOutputFile() string {
	filename := "mr-out"
	file, _ := os.Create(filename)
	file.Close()
	return filename
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

	for _, rt := range c.rts {
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
	intfiles := c.CreateIntermediateFiles(nReduce)
	outfile := c.CreateOutputFile()

	c.wid_counter = 0
	c.nReduce = nReduce
	c.mts = mts
	c.rts = rts
	c.intfiles = intfiles
	c.outfile = outfile

	c.server()
	return &c
}
