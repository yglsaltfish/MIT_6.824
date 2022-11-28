package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskPhase int
type TaskStatus int

const(
	TaskPhase_Map 	TaskPhase = 0
	TaskPhase_Reduce TaskPhase = 1
)

const (
    TaskStatus_New        TaskStatus = 0 //还没有创建
    TaskStatus_Ready      TaskStatus = 1 //进入队列
    TaskStatus_Running    TaskStatus = 2 //已经分配，正在运行
    TaskStatus_Terminated TaskStatus = 3 //运行结束
    TaskStatus_Error      TaskStatus = 4 //运行出错
)

const (
	ScheduleInterval   = time.Millisecond * 500 //扫描任务状态的间隔时间
	MaxTaskRunningTime = time.Second * 10        //每个任务的最大执行时间，用于判断是否超时
)

type Task struct {
    FileName string    //当前任务的文件名
    Phase    TaskPhase //当前任务状态
    Seq      int       //当前的任务序列
    NMap     int       //map任务/file的数量
    NReduce  int       //reduce任务/分区的数量
    Alive    bool      //是否存活
}

type TaskState struct {
    Status    TaskStatus //任务状态
    WorkerId  int        //执行当前Task的workerid
    StartTime time.Time  //任务开始执行的时间
}


type Coordinator struct {
	// Your definitions here.
	files [] 		string
	nReduce 		int 
	taskPhase 		TaskPhase
	taskStates 		[]TaskState
	taskChan 		chan Task
	workerseq  		int
	done 			bool
	muLock 			sync.Mutex
}

func (c *Coordinator) NewOneTask(seq int) Task{
	task := Task{
		FileName: "",
		Phase: c.taskPhase,
		NMap: len(c.files),
		NReduce: c.nReduce,
		Seq: seq,
		Alive: true,
	}
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", c, seq, len(c.files), len(c.taskStates))
	if task.Phase == TaskPhase_Map{
		task.FileName = c.files[seq]
	}
	return task
}

func (c * Coordinator) scanTaskState(){
	DPrintf("scanTaskState...")
	c.muLock.Lock()
	defer c.muLock.Unlock()
	if c.done{
		return
	}

	alldone := true

	for k,v := range c.taskStates{
		switch v.Status{
			case TaskStatus_New:
				alldone = false
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			case TaskStatus_Ready:
				alldone = false
			case TaskStatus_Running:
				alldone = false
				if time.Now().Sub(v.StartTime) > MaxTaskRunningTime{
					c.taskStates[k].Status = TaskStatus_Ready
					c.taskChan <- c.NewOneTask(k)
				}
			case TaskStatus_Terminated:
			case TaskStatus_Error:
				alldone = false
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			default:
				panic("status err")
		}
	}
	if alldone == true{
		if c.taskPhase == TaskPhase_Map{
		DPrintf("init Reduce Task")
		c.taskPhase = TaskPhase_Reduce
		c.taskStates = make([]TaskState, c.nReduce)
	} else{
		log.Println("finished")
		c.done = true
		}
	}
}

func (c * Coordinator)schedule(){
	for !c.done{
		c.scanTaskState()
		time.Sleep(ScheduleInterval)
	}
}


// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetOneTask(args *TaskArgs, reply * TaskReply) error{
	task := <-c.taskChan
	reply.Task = &task
	
	if task.Alive{
		c.muLock.Lock()
		if task.Phase != c.taskPhase{
			c.muLock.Unlock()
			return errors.New("GetOne Task phase neq")
		}

		c.taskStates[task.Seq].WorkerId = args.WorkerId
        c.taskStates[task.Seq].Status = TaskStatus_Running
        c.taskStates[task.Seq].StartTime = time.Now()

		c.muLock.Unlock()
	}
	DPrintf("get one task")
	return nil
}

func (c *Coordinator) RegWorker(args *RegArgs, reply *RegReply) error {
    DPrintf("worker reg!")
    c.muLock.Lock()
    defer c.muLock.Unlock()
    c.workerseq++ 
    reply.WorkerId = c.workerseq
    return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error{
	c.muLock.Lock()
	defer c.muLock.Unlock()

	DPrintf("get report task")

	if c.taskPhase != args.Phase || c.taskStates[args.Seq].WorkerId != args.WorkerId{
		DPrintf("a useless task")
		return nil
	}
	if args.Done {
	c.taskStates[args.Seq].Status = TaskStatus_Terminated
    } else {
        c.taskStates[args.Seq].Status = TaskStatus_Error
    }
	go c.scanTaskState()
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.muLock.Lock()
	defer c.muLock.Unlock()
	return c.done
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		taskPhase: TaskPhase_Map,
        taskStates: make([]TaskState, len(files)),
		workerseq: 0,
		done: false,
	}
	if len(files) > nReduce{
		c.taskChan = make(chan Task, len(files))
	} else { 
		c.taskChan = make(chan Task, nReduce)
	}
	
	go c.schedule()
	c.server()
	return &c
}
