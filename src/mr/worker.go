package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample(mapf, reducef)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// declare an argument structure.
	// args := ExampleArgs{}
	args := GetJobRequest{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := GetJobResponse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetJob", &args, &reply)

	for !reply.IsDone {
		if reply.JobType == "mapper" {
			DoMap(mapf, reply.MapperIndex, reply.MapperFileName, reply.NReduce)
		} else if reply.JobType == "reducer" {
			DoReduce(reducef, reply.ReducerIndex, reply.NMap)
		} else {
			panic("Unexpected reply.")
		}
		call("Coordinator.GetJob", &args, &reply)
	}
}

func DoMap(mapf func(string, string) []KeyValue, mapperIndex int, filename string, NReduce int) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := [][]KeyValue{}
	for i := 0; i < NReduce; i++ {
		intermediate = append(intermediate, []KeyValue{})
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	log.Printf("start reading file %s", filename)
	for _, kv := range kva {
		index := ihash(kv.Key) % NReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	log.Printf("finish reading file")

	// Sort and write file
	for i, kvs := range intermediate {

		sort.Sort(ByKey(kvs))
		oname := fmt.Sprintf("mr-%d-%d", mapperIndex, i)
		tmpname := fmt.Sprintf("tmp-%d-%d", mapperIndex, i)
		log.Printf("start writing file %s", oname)
		path, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		tmpfile, err := ioutil.TempFile(path, tmpname)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encde %v", kv)
			}
		}
		tmpfile.Close()
		err = os.Rename(tmpfile.Name(), oname)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("finish writing file  %s", oname)

	}

	// declare an argument structure.
	// args := ExampleArgs{}
	args := CompleteMapperRequest{
		MapperIndex: mapperIndex,
	}

	// declare a reply structure.
	reply := CompleteMapperResponse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CompleteMapper", &args, &reply)

}

func DoReduce(reducef func(string, []string) string, reducerIndex int, NMap int) {

	oname := fmt.Sprintf("mr-out-%d", reducerIndex)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//

	results := make(map[string][]string)
	var keys []string

	for i := 0; i < NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, reducerIndex)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := results[kv.Key]; ok {
				results[kv.Key] = append(results[kv.Key], kv.Value)
			} else {
				keys = append(keys, kv.Key)
				results[kv.Key] = []string{kv.Value}
			}
		}

	}

	sort.Strings(keys)

	for i := 0; i < len(keys); i++ {
		values := results[keys[i]]
		output := reducef(keys[i], values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", keys[i], output)
	}

	// declare an argument structure.
	// args := ExampleArgs{}
	args := CompleteReducerRequest{
		ReducerIndex: reducerIndex,
	}

	// declare a reply structure.
	reply := CompleteReducerResponse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CompleteReducer", &args, &reply)

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
