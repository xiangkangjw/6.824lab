package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	nMapper       int
	nReducer      int
	mapperStatus  map[int]bool
	fileNames     []string
	reducerStatus map[int]bool
	// need to store map and reduce worker status
	// need to store all the resulted temporary file locations
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetJob(args *GetJobRequest, reply *GetJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	isMapperDone, isReducerDone := true, true
	for _, val := range c.mapperStatus {
		if !val {
			isMapperDone = false
		}
	}
	for _, val := range c.reducerStatus {
		if !val {
			isReducerDone = false
		}
	}
	if isMapperDone && isReducerDone {
		reply.IsDone = true
	} else if !isMapperDone {
		reply.JobType = "mapper"
		for i := 0; i < len(c.mapperStatus); i++ {
			if !(c.mapperStatus[i]) {
				reply.MapperIndex = i
				reply.MapperFileName = c.fileNames[i]
				reply.NReduce = c.nReducer
				break
			}
		}
	} else {
		reply.JobType = "reducer"
		for i, val := range c.reducerStatus {
			if !val {
				reply.ReducerIndex = i
				reply.NMap = c.nMapper
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) CompleteMapper(args *CompleteMapperRequest, reply *CompleteMapperResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapperStatus[args.MapperIndex] = true
	return nil
}

func (c *Coordinator) CompleteReducer(args *CompleteReducerRequest, reply *CompleteReducerResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reducerStatus[args.ReducerIndex] = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, val := range c.mapperStatus {
		if !val {
			ret = false
		}
	}

	for _, val := range c.reducerStatus {
		if !val {
			ret = false
		}
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMapper:       len(files),
		nReducer:      nReduce,
		reducerStatus: make(map[int]bool),
		mapperStatus:  make(map[int]bool),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < nReduce; i++ {
		c.reducerStatus[i] = false
	}

	for i, _ := range files {
		c.mapperStatus[i] = false
	}

	c.fileNames = files

	// Your code here.

	c.server()
	return &c
}
