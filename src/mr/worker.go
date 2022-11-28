package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct{
	workerId int
	mapf func(string, string) []KeyValue
	reduceF func(string, []string) string
}

func (w *worker) run(){
	DPrintf("run")
	for{
		task, err := w.getTask()
		if err != nil{
			log.Print(err.Error())
			continue
		}
		if !task.Alive{
			DPrintf("worker get task not alive")
			return
		}
		w.doTask(*task)

	}
}

func (w * worker) doTask(task Task){
	switch task.Phase{
	case TaskPhase_Map:
		w.doMapTask(task)
	case TaskPhase_Reduce:
		w.doReduceTask(task)
	default:
		DPrintf("task phase err")
	}
}

func (w * worker) doMapTask(task Task){
	cont, err := ioutil.ReadFile(task.FileName)
	if err != nil{
		DPrintf("%v", err)
		w.reportTask(task,false)
		return
	}
	kvs := w.mapf(task.FileName, string(cont))
	partions := make([][]KeyValue, task.NReduce)
	for _,kv := range kvs{
		pid := ihash(kv.Key) %task.NReduce
		partions[pid] = append(partions[pid], kv)
	}
	for k,v := range partions{
		fileName := w.getReduceName(task.Seq, k)
		file,err := os.Create(fileName)
		if err != nil{
			DPrintf("create file fail")
			w.reportTask(task, false)
			return
		}
		encoder := json.NewEncoder(file)
		for _, kv := range v {
			if err := encoder.Encode(&kv); err != nil {
				DPrintf("encode  kvs to file-%v  fail in doMapTask. %v", fileName, err)
				w.reportTask(task, false)
			}	
        }
		if err := file.Close(); err != nil {
            DPrintf("close file-%v fail in doMapTask. %v", fileName, err)
            w.reportTask(task, false)
        }
	}
	w.reportTask(task,true)

}


func (w *worker) doReduceTask(task Task) {
    maps := make(map[string][]string)

    for i := 0; i < task.NMap; i++ {
        fileName := w.getReduceName(i, task.Seq)
        file, err := os.Open(fileName)
        if err != nil {
            DPrintf("open  file-%v fail in doReduceTask. %v", fileName, err)
            w.reportTask(task, false)
            return
        }
        decoder := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := decoder.Decode(&kv); err != nil {
                break
            }
            if _, ok := maps[kv.Key]; !ok {
                maps[kv.Key] = make([]string, 0)
            }
            maps[kv.Key] = append(maps[kv.Key], kv.Value)
        }
    }

    res := make([]string, 0)
    for k, v := range maps {
        len := w.reduceF(k, v)
        res = append(res, fmt.Sprintf("%v %v\n", k, len))
    }

    fileName := w.getMergeName(task.Seq)
    if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil {
    DPrintf("write file-%v in doReduceTask. %v", fileName, err)
        w.reportTask(task, false)
    }

    w.reportTask(task, true)
}

func (w *worker) getReduceName(mapId, partitionId int) string {
    return fmt.Sprintf("mr-kv-%d-%d", mapId, partitionId)
}

//reduce任务时获取要输出的文件名
func (w *worker) getMergeName(partitionId int) string {
    return fmt.Sprintf("mr-out-%d", partitionId)
}

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
	worker := worker{
		mapf: mapf,
		reduceF: reducef,
	}
	worker.register()
	worker.run()
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	return
}





// rpc function

func(w *worker) register(){
	DPrintf("register")
	args := &RegArgs{}
	reply := &RegReply{}

	if err := call("Coordinator.RegWorker", &args, &reply); !err{
		log.Fatal("worker register error", err)
	}
	w.workerId = reply.WorkerId
}

func (w *worker) getTask() (* Task, error){
	args := TaskArgs{WorkerId: w.workerId}
	reply := TaskReply{}
	if err := call("Coordinator.GetOneTask", &args, & reply);!err{
		log.Fatal("worker getTask error", err)
	}
	return reply.Task, nil
}

func (w *worker) reportTask(task Task, done bool){
	args := ReportTaskArgs{
		WorkerId: w.workerId,
		Phase:  task.Phase,
		Seq: task.Seq,
		Done: done,
	}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, & reply);!ok{
		DPrintf("report task fail")
	}
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
