package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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
	for {
		taskInfo := CallAskTask()
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapf, taskInfo)
			break
		case TaskReduce:
			workerReduce(reducef, taskInfo)
			break
		case TaskWait:
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master's task is all completed.")
			return
		default:
			panic("Invalid Task Stat receiver from worker")
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func CallAskTask() *TaskInfo {
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask", &args, &reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Master.TaskDone", taskInfo, reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	fmt.Printf("Got assigned map task on %vth file %v\n", taskInfo.FileIndex, taskInfo.FileName)

	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("can't open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := taskInfo.NReduce
	outPrefix := "mr-tmp/mr-"
	outPrefix += strconv.Itoa(taskInfo.FileIndex)
	outPrefix += "-"
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)

	for outIndex := 0; outIndex < nReduce; outIndex++ {
		outFiles[outIndex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[outIndex] = json.NewEncoder(outFiles[outIndex])
	}

	for _, kv := range intermediate {
		outIndex := ihash(kv.Key) % nReduce
		file = outFiles[outIndex]
		enc := fileEncs[outIndex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	for outIndex, file := range outFiles {
		outName := outPrefix + strconv.Itoa(outIndex)
		oldPath := filepath.Join(file.Name())
		os.Rename(oldPath, outName)
		file.Close()
	}

	CallTaskDone(taskInfo)
}

func workerReduce(reducef func(string, []string) string, taskInfo *TaskInfo) {
	fmt.Printf("Got assigned reduce task on part %v\n", taskInfo.PartIndex)
	outName := "mr-out-" + strconv.Itoa(taskInfo.PartIndex)
	//fmt.Printf("%v\n", taskInfo)

	// read from output files from map tasks

	innamePrefix := "mr-tmp/mr-"
	innameSuffix := "-" + strconv.Itoa(taskInfo.PartIndex)

	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < taskInfo.NFiles; index++ {
		inname := innamePrefix + strconv.Itoa(index) + innameSuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//fmt.Printf("%v\n", err)
				break
			}
			//fmt.Printf("%v\n", kv)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	//ofile, err := os.Create(outname)
	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outName, err)
		panic("Create file error")
	}
	//fmt.Printf("%v\n", intermediate)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outName)
	ofile.Close()
	// acknowledge master
	CallTaskDone(taskInfo)
}
