package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	// "time"
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

	res, err := CallHandshake()
	if err != nil {
		fmt.Println("Worker: Handshake failed.")
	}
	wid := res.WorkerID

	res, _ = CallGetTask(wid)

	for res.TaskType != NO_JOB {
		if res.TaskType == MAP {
			mt := MapTask{}
			mt.wid = wid
			mt.filename = res.TaskInfo
			mt.state = IN_PROGRESS
			// fmt.Println("Executing map")
			ExecuteMap(mt, mapf)
		}

		if res.TaskType == REDUCE {
			rt := ReduceTask{}
			rt.wid = wid
			rt.filename = res.TaskInfo
			rt.state = IN_PROGRESS
			rt.no = res.ReduceNo
			ExecuteReduce(rt, reducef)
		}
		res, _ = CallGetTask(wid)
	}
}

func ExecuteMap(mt MapTask, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	filename, content, err := ProcessFile(mt.filename)
	if err != nil {
		fmt.Println("Worker::ExecuteMap: ProcessFile failed.")
	}
	kva := mapf(filename, content)
	mt.state = DONE

	// time.Sleep(800*time.Millisecond)

	err = CallSetMapStatus(mt.wid, DONE)
	if err != nil {
		fmt.Println("Worker:ExecuteMap: SetMapStatus failed.")
	}

	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	DistributeIntermedite(intermediate)
}

func ExecuteReduce(rt ReduceTask, reducef func(string, []string) string) {
	kvs := ConvertTaskContent(rt.filename)

	oname := "mr-out-" + strconv.Itoa(rt.no)
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

	CallSetReduceStatus(rt.wid, DONE)
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

func CallGetTask(wid int) (RPCResponse, error) {
	req := RPCRequest{}
	res := RPCResponse{}

	req.WorkerID = wid

	ok := call("Coordinator.GetTask", &req, &res)

	if ok {
		return res, nil
	} else {
		return res, rpc.ErrShutdown
	}
}

func CallSetMapStatus(wid, status int) error {
	req := RPCRequest{}
	res := RPCResponse{}

	req.WorkerID = wid
	req.MapStatus = status

	ok := call("Coordinator.SetMapStatus", &req, &res)

	if ok {
		return nil
	} else {
		return rpc.ErrShutdown
	}
}

func CallSetReduceStatus(wid, status int) error {
	req := RPCRequest{}
	res := RPCResponse{}

	req.WorkerID = wid
	req.ReduceStatus = status

	ok := call("Coordinator.SetReduceStatus", &req, &res)

	if ok {
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

	var kvs []KeyValue

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}

	sort.Sort(ByKey(kvs))

	return kvs

	// // Read the JSON data from the file
	// jsonData, err := io.ReadAll(file)
	// if err != nil {
	// 	fmt.Println("Error reading file:", err)
	// }

	// // Check if JSON data is empty
	// if len(jsonData) == 0 {
	// 	fmt.Println("Error: JSON data is empty")
	// }

	// // Unmarshal the JSON data into a slice of KeyValue structs
	// var kvs []KeyValue
	// if err := json.Unmarshal(jsonData, &kvs); err != nil {
	// 	fmt.Println("Error unmarshalling JSON:", err)
	// }
	// return kvs
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
