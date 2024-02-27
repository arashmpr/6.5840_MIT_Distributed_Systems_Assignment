package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

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
		fmt.Println("We are in Worker")

		reply, err := CallAssignTask()

		if err != nil {
			fmt.Println("RPC Failed.")
		}

		if reply.TaskType == MAP {
			fmt.Println("You are okay, do the rest later")
		}
		// if reply.TaskType == MAP {
		// 	fmt.Println("till now we are okay")
		// }
		// CallExample()

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//callsomething()

}

func CallAssignTask() (Reply, error) {

	args := Args{}
	reply := Reply{}

	args.Type = MAP

	ok := call("Coordinator.AssignTask", &args, &reply)

	if ok {
		fmt.Println("This worker called OK and task type is", reply.TaskType)
		return reply, nil
	} else {
		fmt.Println("Call failed.")
		return reply, rpc.ErrShutdown
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
