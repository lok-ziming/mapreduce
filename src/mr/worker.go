package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const midResult = "mr-%d-%d"

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
	// Your worker implementation here.
	interval := rand.Intn(1000)
	for {
		<-time.After(time.Duration(interval) * time.Millisecond)
		err := Run(mapf, reducef)
		if err.Error() == "master is not available" {

		} else if err != nil {
			interval = min(10000, interval*2)
			continue
		}

	}

}

func Run(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return fmt.Errorf("master is not available")
	}
	if reply.Task.Id == -1 {
		return fmt.Errorf("no task available")
	}
	if Map == reply.Task.Type {
		content, err := os.ReadFile(reply.Task.Files[0])
		if err != nil {
			return fmt.Errorf("failed to read file %s", reply.Task.Files[0])
		}
		kv := mapf(reply.Task.Files[0], string(content))
		kvs := make([][]KeyValue, reply.nReduce)
		for i := range kvs {
			kvs[i] = make([]KeyValue, 0, 10)
		}
		for _, iter := range kv {
			reduceTaskId := ihash(iter.Key) % reply.nReduce
			kvs[reduceTaskId] = append(kvs[reduceTaskId], iter)
		}
		var wg sync.WaitGroup
		mid := make([]string, reply.nReduce)
		for i, kv := range kvs {
			if len(kv) == 0 {
				continue
			}
			fileName := fmt.Sprintf(midResult, reply.Task.Id, i)
			mid = append(mid, fileName)
			wg.Add(1)
			go func(kv []KeyValue, fileName string) {
				defer wg.Done()
				file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					log.Printf("failed to create file %s: %v", fileName, err)
					return
				}
				defer file.Close()
				for _, iter := range kv {
					fmt.Fprintf(file, "%s %s\n", iter.Key, iter.Value)
				}
			}(kv, fileName)
		}
		wg.Wait()
		taskResult := SetTaskArgs{
			TaskResult: TaskResult{
				Id:      reply.Task.Id,
				Type:    reply.Task.Type,
				Version: reply.Task.Version,
				Status:  Finished,
				Result:  mid,
			},
		}
		ok = call("Coordinator.SetTaskResult", &taskResult, &SetTaskReply{})
		if !ok {
			return fmt.Errorf("master is not available")
		}
	} else if Reduce == reply.Task.Type {
		log.Printf("Received Reduce Task: %v\n", reply.Task.Id)
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
