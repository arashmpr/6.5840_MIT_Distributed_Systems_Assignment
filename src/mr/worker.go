package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

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
		// require more work (ping the worker, do the map reduce)
		fmt.Println("Starting Worker")

		worker := WorkerSpec{}

		res, err := CallGetWorkerID()
		if err != nil {
			fmt.Println("Worker: GetWorkerID failed.")
		}
		worker.wid = res.WorkerID
		fmt.Println("Worker ID is : ", worker.wid)

		res, err = CallCheckMapStatus()
		if err != nil {
			fmt.Println("Worker: CheckMapStatus failed.")
		}

		for res.MapStatus == IDLE {
			time.Sleep(time.Second)
		}

}

func CallGetWorkerID() (RPCResponse, error) {

	req := RPCRequest{}
	res := RPCResponse{}

	ok := call("Coordinator.GetWorkerID", &req, &res)

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
