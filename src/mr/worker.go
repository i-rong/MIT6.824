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
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task struct {
	TaskType   TaskType // 表示当前这个任务是干什么的 可选的任务类型有 MapTask ReduceTask WaitingTask ExitTask
	TaskId     int      // 当前任务的id 所有任务的TaskId都是不同的
	ReducerNum int      // 做reduce任务的woker数量 在hash的时候要用到 需要用它来%
	FileNames  []string // 对于map task来说 传入的就是一个文件的名字 但是对于reduce task来说 传入的是多个文件的名称 所以对于map task 来说 我们处理第一个下标的就好
}

type TaskType int

// 四种task
const (
	MapTask     TaskType = iota // 表示这个worker要去做Map task
	ReduceTask                  // 表示这个woker要去做Reduce task
	WaitingTask                 // 表示需要做的任务都分配出去了 但是这些任务还没有完成 当前这个worker等待就好
	ExitTask                    // 表示所有的任务都做完了 worker可以退出了
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workingFlag := true
	for workingFlag {
		task := getTask() // 请求Coordinator分配一个任务
		switch task.TaskType {
		case MapTask:
			{
				doMapTask(mapf, &task) // 如果是maptask 那么这个woker就去做map task
				callDone(&task)        // 做完了 需要将这个任务标记为做完了
			}
		case ReduceTask:
			{
				doReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaitingTask:
			{
				fmt.Printf("All tasks are in progress! Task %d is waiting.\n", task.TaskId)
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Printf("Task %d is terminated...\n", task.TaskId)
				workingFlag = false
			}
		default:
			{
				panic("undefined TaskType!!!")
			}
		}
	}
}

// 首先明确map task是在做什么
// 一个map task将一个传进来的文件分成ReducerNum个中间文件 这些中间文件里面的内容是一个一个的键值对 这个时候还没有排序
// 比如现在传进来一个文件 我们用一个woker去做这个任务(这个woker的编号是1)在做map task 定义了10个ReducerNum 那么它就会生成下面这些文件
// mr-tmp-0-0 mr-tmp-0-1 mr-tmp-0-2 ... mr-tmp-0-9
// 下面的代码主要参考mrsequential.go
func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.FileNames[0]

	file, err := os.Open(filename) // 对于map reduce来说 我们只关心FileSlice中的第一个file
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}
	file.Close()

	// 下面对从filename中取出来的content做map操作
	intermediate := mapf(filename, string(content))

	reducerNum := task.ReducerNum

	hashBuckets := make([][]KeyValue, reducerNum) // 构造一个hash桶 有reducerNum个 每个里面都是很多的KeyValue对

	for _, kv := range intermediate {
		hashBuckets[ihash(kv.Key)%reducerNum] = append(hashBuckets[ihash(kv.Key)%reducerNum], kv)
	}

	for i := 0; i < reducerNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		// 编码进这个文件中
		for _, kv := range hashBuckets[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

// 参考mrsequence.go编写该函数
func doReduceTask(reducef func(string, []string) string, task *Task) {
	fileNames := task.FileNames
	intermediate := sortContent(fileNames) // 返回的是排好序的键值对
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fileName := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), fileName)
}

func sortContent(fileNames []string) []KeyValue {
	var ret []KeyValue
	for _, fileName := range fileNames {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			ret = append(ret, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(ret))
	return ret
}

// call RPC将这个任务标记为做完了
func callDone(task *Task) {
	args := task
	reply := Task{}

	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Printf("Task %d finished.\n", task.TaskId)
	} else {
		fmt.Printf("Call failed in callDone\n")
	}
}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("Coordinator assigned a task, and it is a %d task\n", reply.TaskType)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
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
