package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu       sync.Mutex
	nMapper  int
	nReducer int
	// 0 -> not started,  1 -> in progress, 2 -> done
	mapperStatus  map[int]int
	fileNames     []string
	reducerStatus map[int]int
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
		if val != 2 {
			isMapperDone = false
		}
	}
	for _, val := range c.reducerStatus {
		if val != 2 {
			isReducerDone = false
		}
	}
	if isMapperDone && isReducerDone {
		reply.IsDone = true
	} else if !isMapperDone {
		for i := 0; i < len(c.mapperStatus); i++ {
			if c.mapperStatus[i] == 0 {
				reply.JobType = "mapper"
				reply.MapperIndex = i
				reply.MapperFileName = c.fileNames[i]
				reply.NReduce = c.nReducer
				c.mapperStatus[i] = 1

				go func(mapIndex int, c *Coordinator) {
					time.Sleep(time.Second * 5)
					c.mu.Lock()
					defer c.mu.Unlock()

					if c.mapperStatus[mapIndex] == 1 {
						c.mapperStatus[mapIndex] = 0
					}
				}(i, c)
				break
			}
		}
	} else {
		for i, val := range c.reducerStatus {
			if val == 0 {
				reply.JobType = "reducer"
				reply.ReducerIndex = i
				reply.NMap = c.nMapper

				c.reducerStatus[i] = 1
				go func(reducerIndex int, c *Coordinator) {
					time.Sleep(time.Second * 5)
					c.mu.Lock()
					defer c.mu.Unlock()

					if c.reducerStatus[reducerIndex] == 1 {
						c.reducerStatus[reducerIndex] = 0
					}
				}(i, c)
				break
			}
		}
	}
	reply.ShouldWait = true
	return nil
}

func (c *Coordinator) CompleteMapper(args *CompleteMapperRequest, reply *CompleteMapperResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapperStatus[args.MapperIndex] = 2
	return nil
}

func (c *Coordinator) CompleteReducer(args *CompleteReducerRequest, reply *CompleteReducerResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reducerStatus[args.ReducerIndex] = 2
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
		if val != 2 {
			ret = false
		}
	}

	for _, val := range c.reducerStatus {
		if val != 2 {
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
		reducerStatus: make(map[int]int),
		mapperStatus:  make(map[int]int),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < nReduce; i++ {
		c.reducerStatus[i] = 0
	}

	for i, _ := range files {
		c.mapperStatus[i] = 0
	}

	c.fileNames = files

	// Your code here.

	c.server()
	return &c
}
