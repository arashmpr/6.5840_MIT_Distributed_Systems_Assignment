package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	"encoding/json"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// require more work (ping the worker, do the reduce)
	fmt.Println("Starting Worker")

	worker := WorkerSpec{}

	res, err := CallHandshake()
	if err != nil {
		fmt.Println("Worker: Handshake failed.")
	}
	worker.wid = res.WorkerID
	fmt.Println("Worker ID is : ", worker.wid)

	res, err = CallCheckMapStatus()
	if err != nil {
		fmt.Println("Worker: CheckMapStatus failed.")
	}

	for res.MapStatus != DONE {
		fmt.Println("From the top")
		if res.MapStatus == IN_PROGRESS {
			time.Sleep(time.Second)
			continue
		}
		mt, err := CallGetMapTask()
		if err != nil {
			fmt.Println("Worker: GetMapTask failed.")
		}

		mt.state = IN_PROGRESS

		intermediate := []KeyValue{}
		filename, content, err := ProcessFile(mt.filename)
		if err != nil {
			fmt.Println("Worker: ProcessFile failed.")
		}
		kva := mapf(filename, content)
		mt.state = DONE

		err = CallSetMapStatus(worker.wid, DONE)
		if err != nil {
			fmt.Println("Worker: SetMapStatus failed.")
		}

		//intermediate starts
		intermediate = append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))
		DistributeIntermedite(intermediate)

		fmt.Println("Worker: Map part is done.")

		res, err = CallCheckMapStatus()
		if err != nil {
			fmt.Println("Worker: CheckMapStatus failed.")
		}
		break
	}

	//free the worker

	err = CallFreeWorker(worker.wid)
	if err != nil {
		fmt.Println("Worker::FreeWorker: failed.")
	}

	// start reduce part
	res, err = CallCheckReduceStatus()
	if err != nil {
		fmt.Println("Worker: CheckReduceStatus failed.")
	}

	for res.ReduceStatus != DONE {
		fmt.Println("From the top in Reduce")
		if res.ReduceStatus == IN_PROGRESS {
			time.Sleep(time.Second)
			continue
		}
		rt, err := CallGetReduceTask()
		if err != nil {
			fmt.Println("Worker: GetReduceTask failed.")
		}
		fmt.Println("My Reduce task is ", rt.filename)

		kvs := ConvertTaskContent(rt.filename)

		oname := "mr-out"
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)
	
			fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
			i = j
		}

		
		fmt.Println("Till here works")
	}
}

func CallHandshake() (RPCResponse, error) {

	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.Handshake", &req, &res)

	if ok {
		return res, nil
	} else {
		return res, rpc.ErrShutdown
	}
}

func CallCheckMapStatus() (RPCResponse, error) {

	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.CheckMapStatus", &req, &res)

	if ok {
		return res, nil
	} else {
		return res, rpc.ErrShutdown
	}
}

func CallCheckReduceStatus() (RPCResponse, error) {

	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.CheckReduceStatus", &req, &res)

	if ok {
		return res, nil
	} else {
		return res, rpc.ErrShutdown
	}
}

func CallGetMapTask() (MapTask, error) {
	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.GetMapTask", &req, &res)

	if ok {
		var mt MapTask
		mt.wid = res.WorkerID
		mt.filename = res.TaskInfo
		return mt, nil
	} else {
		return MapTask{}, rpc.ErrShutdown
	}
}

func CallGetReduceTask() (ReduceTask, error) {
	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.GetReduceTask", &req, &res)

	if ok {
		var rt ReduceTask
		rt.wid = res.WorkerID
		rt.filename = res.TaskInfo
		return rt, nil
	} else {
		return ReduceTask{}, rpc.ErrShutdown
	}
}

func CallSetMapStatus(wid, status int) error {
	req := RPCRequest{}
	res := RPCResponse{}

	req.WorkerID = wid
	req.MapStatus = status

	ok := call("Coordinator.SetMapStatus", &req, &res)

	if ok {
		fmt.Println("Worker::CallSetMapStatus: Map Status is Done.")
		return nil
	} else {
		return rpc.ErrShutdown
	}
}

func CallFreeWorker(wid int) error {
	req := RPCRequest{}
	res := RPCResponse{}

	req.WorkerID = wid

	ok := call("Coordinator.FreeWorker", &req, &res)

	if ok {
		fmt.Println("Worker::CallSetMapStatus: Worker is free.")
		return nil
	} else {
		return rpc.ErrShutdown
	}

}

func ProcessFile(filename string) (string, string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", "", rpc.ErrShutdown
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", "", rpc.ErrShutdown
	}
	file.Close()
	return filename, string(content), nil
}

func DistributeIntermedite(intermediate []KeyValue) {
	req := RPCRequest{}
	res := RPCResponse{}

	req.Intermediate = intermediate

	ok := call("Coordinator.DistributeIntermedite", &req, &res)

	if ok {
		fmt.Println("Intermediate is distributed.")
	} else {
		fmt.Println("SendIntermediateToCoordinator failed.")
	}
}

func ConvertTaskContent(filenamen string) []KeyValue {
	// Open the JSON file
	file, err := os.Open(filenamen)
	if err != nil {
		fmt.Println("Error opening file:", err)
	}
	defer file.Close()

	// Read the JSON data from the file
    jsonData, err := io.ReadAll(file)
    if err != nil {
        fmt.Println("Error reading file:", err)
    }

    // Check if JSON data is empty
    if len(jsonData) == 0 {
        fmt.Println("Error: JSON data is empty")
    }

    // Unmarshal the JSON data into a slice of KeyValue structs
    var kvs []KeyValue
    if err := json.Unmarshal(jsonData, &kvs); err != nil {
        fmt.Println("Error unmarshalling JSON:", err)
    }
	return kvs
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
