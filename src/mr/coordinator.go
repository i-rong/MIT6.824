package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// var mu sync.Locker

type Coordinator struct {
	ReducerNum        int        // 记录Coordinator准备分配几个Reducer
	TaskId            int        // 记录下一个分配的任务的taskid
	DistPhase         Phase      // 记录当前分配任务的阶段
	MapTaskChannel    chan *Task // 存放分配出去的Map task
	ReduceTaskChannel chan *Task // 存放分配出去的Reduce task

	TaskHolder TaskHolder // 存放所有的task
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Alldone
)

type TaskStatus int

const (
	Operating TaskStatus = iota // 表示这个任务正在被某一个worker执行
	Waiting                     // 表示这个任务等待被某一个worker执行
	Done                        // 表示这个任务已经被执行完毕
)

type TaskHolder struct {
	Meta map[int]*TaskInfo
}

type TaskInfo struct {
	StartTime  time.Time  // 任务开始的时间
	TaskStatus TaskStatus // 当前task的状态 有三种 正在执行 等待被执行 执行完毕
	TaskAddr   *Task      // 当前task的地址
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
	if c.DistPhase == Alldone {
		fmt.Printf("All tasks are finished, the coordinator will exit.\n")
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		TaskId:            0,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),

		TaskHolder: TaskHolder{
			Meta: make(map[int]*TaskInfo, len(files)+nReduce), // task的数量是所有map的数量 + reduce的数量
		},
	}

	c.makeMapTasks(files)
	c.server()

	go c.CrashDetector()
	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		if c.DistPhase == Alldone { // 如果都做完了 那就结束
			break
		}

		for _, taskInfo := range c.TaskHolder.Meta { // 遍历每一个任务 看它们的启动时间距离现在多久
			if taskInfo.TaskStatus == Operating {
				fmt.Printf("Task %d is working\n", taskInfo.TaskAddr.TaskId)
			}
			if taskInfo.TaskStatus == Operating && time.Since(taskInfo.StartTime) > time.Second*9 {
				fmt.Printf("Task %d crashed, and it is a %d task\n", taskInfo.TaskAddr.TaskId, taskInfo.TaskAddr.TaskType)

				if taskInfo.TaskAddr.TaskType == MapTask {
					taskInfo.TaskStatus = Waiting
					c.MapTaskChannel <- taskInfo.TaskAddr
				} else if taskInfo.TaskAddr.TaskType == ReduceTask {
					taskInfo.TaskStatus = Waiting
					c.ReduceTaskChannel <- taskInfo.TaskAddr
				}
			}
		}
	}
}

// 对传进来的files进行处理，将每一个文件名都初始化成一个map task
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		mapTaskId := c.genTaskId() // coordinator分配唯一的任务编号
		task := Task{
			TaskType:   MapTask,
			TaskId:     mapTaskId,
			ReducerNum: c.ReducerNum,
			FileNames:  []string{file}, // 一个字符串数组 并将第一个初始化为file
		}

		c.TaskHolder.AcceptTask(&task)
		fmt.Printf("The Coordinator makes a map task, which task id is %d\n", task.TaskId)
		c.MapTaskChannel <- &task
	}
}

func (TaskHolder *TaskHolder) AcceptTask(task *Task) bool {
	taskId := task.TaskId
	tmp := TaskHolder.Meta[taskId]
	if tmp != nil { // 说明已经有一个task为taskId的放在TaskHolder里面了
		fmt.Printf("TaskHolder already has the task %d.\n", taskId)
		return false
	} else {
		taskInfo := TaskInfo{
			TaskStatus: Waiting, // task的初始状态为等待执行
			TaskAddr:   task,
		}
		TaskHolder.Meta[taskId] = &taskInfo
		return true
	}
}

func (c *Coordinator) genTaskId() int {
	ret := c.TaskId // 将当前这个数字分配出去
	c.TaskId++      // 下一个任务的id
	return ret
}

// Coordinator分配任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// 分配任务的时候应该上锁 防止多个worker竞争同一个task 并使用defer回退解锁
	// mu.Lock()
	// defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase: // 如果是MapPhase 那么需要给这个worker分配map任务 需要从MapTaskChannel中取出来
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel // 相当于从队头取出一个task给这个worker
				taskId := reply.TaskId
				taskStatus := c.getTaskStatus(taskId)
				if taskStatus == Operating { // 这个任务正在执行
					fmt.Printf("The task %d is already running.\n", taskId)
				} else if taskStatus == Waiting { // 这个任务正在等待被执行
					c.setTaskStatus(taskId)
					fmt.Printf("The task %d begins to run.\n", taskId)
				} else if taskStatus == Done { // 这个任务已经被执行完毕了
					fmt.Printf("The task %d is done.\n", taskId)
				}
			} else { // 没有任务了 说明任务都在被做 或者已经做完了 就把这个worker设置为Waiting
				reply.TaskType = WaitingTask
				if c.checkAllTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				taskId := reply.TaskId
				taskStatus := c.getTaskStatus(taskId)
				if taskStatus == Operating { // 这个任务正在执行
					fmt.Printf("The task %d is already running.\n", taskId)
				} else if taskStatus == Waiting { // 这个任务正在等待被执行
					c.setTaskStatus(taskId)
					fmt.Printf("The task %d begins to run.\n", taskId)
				} else if taskStatus == Done { // 这个任务已经被执行完毕了
					fmt.Printf("The task %d is done.\n", taskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.checkAllTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case Alldone:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) getTaskStatus(taskId int) TaskStatus {
	var ret TaskStatus
	taskHolder := c.TaskHolder
	taskInfo := taskHolder.Meta[taskId]
	if taskInfo == nil { // 找不到这个task
		fmt.Printf("The task %d is not in TaskHolder.\n", taskId)
	} else {
		ret = taskInfo.TaskStatus
	}
	return ret
}

func (c *Coordinator) setTaskStatus(taskId int) {
	c.TaskHolder.Meta[taskId].TaskStatus = Operating
	c.TaskHolder.Meta[taskId].StartTime = time.Now() // 初始化开始时间
}

func (c *Coordinator) checkAllTaskDone() bool { // 检查是不是所有当前阶段的任务都完成了 这里的任务只有两种状态 一种是正在执行 一种是已经做完了 不可能有等待执行的任务
	var (
		taskMapDoneCnt      = 0
		taskMapUnDoneCnt    = 0
		taskReduceDoneCnt   = 0
		taskReduceUnDoneCnt = 0
	)
	taskHolder := c.TaskHolder
	for _, taskInfo := range taskHolder.Meta {
		if taskInfo.TaskAddr.TaskType == MapTask {
			if taskInfo.TaskStatus == Done {
				taskMapDoneCnt++
			} else {
				taskMapUnDoneCnt++
			}
		} else if taskInfo.TaskAddr.TaskType == ReduceTask {
			if taskInfo.TaskStatus == Done {
				taskReduceDoneCnt++
			} else {
				taskReduceUnDoneCnt++
			}
		}
	}

	fmt.Printf("taskMapDoneCnt: %d, taskMapUnDoneCnt: %d, taskReduceDoneCnt: %d, taskReduceUnDoneCnt: %d.\n", taskMapDoneCnt, taskMapUnDoneCnt, taskReduceDoneCnt, taskReduceUnDoneCnt)
	if (taskMapDoneCnt > 0 && taskMapUnDoneCnt == 0) && (taskReduceDoneCnt == 0 && taskReduceUnDoneCnt == 0) { // reduce任务还没有开始做
		// map tasks all done
		return true
	} else if taskReduceDoneCnt > 0 && taskReduceUnDoneCnt == 0 { // reduce任务做完了
		return true
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	switch c.DistPhase {
	case MapPhase:
		{
			c.makeReduceTasks()
			c.DistPhase = ReducePhase
			break
		}
	case ReducePhase:
		{
			c.DistPhase = Alldone
			break
		}
	case Alldone:
		{
			break
		}
	default:
		{
			panic("Undefined Phase!\n")
		}
	}
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	taskId := args.TaskId
	taskHolder := c.TaskHolder.Meta
	taskInfo := taskHolder[taskId]
	if taskInfo == nil {
		fmt.Printf("The task %d is not exists.\n", taskId)
	} else if taskInfo.TaskStatus == Operating {
		taskInfo.TaskStatus = Done
		fmt.Printf("The task %d is done.\n", taskId)
	} else if taskInfo.TaskStatus == Done {
		fmt.Printf("The task %d is done before.\n", taskId)
	} else if taskInfo.TaskStatus == Waiting {
		fmt.Printf("The task %d's status should not be here.\n", taskId)
	}
	return nil
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.genTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			FileNames: selectReduceName(i), // 选择这个reduce task需要传入的临时文件的名称
		}

		c.TaskHolder.AcceptTask(&task) // 将task放进TaskHolder中
		fmt.Printf("The coordinator makes a reduce task, which task id is %d\n", task.TaskId)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reducerId int) []string {
	var ret []string
	path, _ := os.Getwd()
	fileInfos, _ := ioutil.ReadDir(path)
	for _, fileInfo := range fileInfos {
		if strings.HasPrefix(fileInfo.Name(), "mr-tmp") && strings.HasSuffix(fileInfo.Name(), strconv.Itoa(reducerId)) {
			ret = append(ret, fileInfo.Name())
		}
	}
	return ret
}
