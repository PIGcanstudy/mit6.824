package mr

import (
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const minSplitSize = 16 * 1024 * 1024 // 16MB
const maxSplitSize = 64 * 1024 * 1024 // 64MB

var (
	Mu       sync.Mutex
	numFiles int
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type TaskStatus int

const (
	TaskPending TaskStatus = iota // 任务等待处理
	ReduceTaskInProgress
	ReduceTaskDone
	MapTaskInProgress // Map任务正在进行中
	MapTaskDone       // Map任务已经完成
)

type TaskType int

const (
	Map    TaskType = iota // Map任务
	Reduce                 // Reduce任务
	Wait                   // 通知Worker等待任务
	Exit                   // 通知Worker退出
	Active                 // 运行状态
)

type Task struct {
	Id         int        // 用来标识一个任务
	NReduce    int        // 用来存储Reduce任务的个数 （由于运行在不同进程使用全局变量行不通了，当然也可以使用PRC来传递此信息）
	Input      string     // 输入分割后的内容
	Tasktype   TaskType   // 用来存储任务的类型
	Taskstatus TaskStatus // 用来存储任务的状态
	MiddleData []string   // 用来存储中间结果被存放在哪个文件的路径
	Output     string     // 处理后输出的结果
}

type Coordinator struct {
	// Your definitions here.
	NReduce  int              // 用来存储Reduce任务的个数
	nMap     int              // 用来存储Map任务的个数
	WorkerId int              // 用来存储Worker的个数（用来唯一表示一个Worker）
	Workers  map[int]struct{} // 用来存储活跃的Worker
	Status   TaskType         // 用来存储Coordinator的阶段
	Tasks    map[int]Task     // 用来存储任务的数组
	nTask    int              // 用来存储任务的个数

	heartBeatChannel chan bool // 用来给Worker发送心跳包
	TasksQueue       chan Task // 用来给Worker发送任务
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 分发任务的PRC处理函数
func (c *Coordinator) AssignTask(args *NullArgs, reply *Task) error {
	// 直接这么写会导致协调器阻塞，rpc调用就不会返回了，这种设计不好
	//*reply = <-c.TasksQueue // 从任务队列中取出一个任务

	// 以下的写法可以防止协调器阻塞，但是会导致rpc调用返回后，任务队列中没有任务，导致worker阻塞而这正好是我们想要的效果
	// 由于要使用len函数获取管道的长度，所以需要加锁
	Mu.Lock()
	defer Mu.Unlock()

	log.Println("AssignTask called, len(TasksQueue):", len(c.TasksQueue))

	if len(c.TasksQueue) > 0 {
		// 任务队列不为空，取出一个任务
		*reply = <-c.TasksQueue
	} else if c.Status == Exit {
		// 提示Worker退出
		*reply = Task{Tasktype: Exit}
	} else {
		// 提示Worker等待任务
		*reply = Task{Tasktype: Wait}
	}

	return nil
}

// Map任务完成
func (c *Coordinator) MapTaskComplated(task *Task, reply *NullReply) error {
	Mu.Lock()
	defer Mu.Unlock()

	task.Taskstatus = MapTaskDone // 任务状态改为MapTaskDone

	task.Tasktype = Reduce // 任务类型改为Reduce

	c.createReduceTask(task) // 创建Reduce任务

	c.Tasks[task.Id] = *task // 更新任务数组(此时任务由map转为reduce任务)

	return nil
}

// Reduce任务完成
func (c *Coordinator) ReduceTaskComplated(task *Task, reply *NullReply) error {
	Mu.Lock()
	log.Println("ReduceTaskComplated called, task.Id:", task.Id)
	task.Taskstatus = ReduceTaskDone // 任务状态改为ReduceTaskDone
	c.Tasks[task.Id] = *task         // 更新任务数组
	Mu.Unlock()
	go func() {
		Mu.Lock()
		defer Mu.Unlock()
		if c.CheckReduceTaskDone() {
			c.Status = Exit // 状态改为Exit
		}
	}()
	return nil
}

// 获取队列长度
func (c *Coordinator) GetQueueLen(args *NullArgs, reply *QueueLenReply) error {
	Mu.Lock()
	defer Mu.Unlock()
	reply.Len = len(c.TasksQueue)
	return nil
}

func (c *Coordinator) CheckReduceTaskDone() bool {
	// 遍历任务数组，检查是否所有Reduce任务都完成
	flag := true
	for _, task := range c.Tasks {
		log.Println(task)
		if task.Tasktype == Map {
			flag = false
			break
		} else if task.Tasktype == Reduce && task.Taskstatus != ReduceTaskDone {
			flag = false
			break
		}
	}
	return flag
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	log.Println("RPC server started.")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 参数nil表示使用默认的多路复用器来处理HTTP请求
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	Mu.Lock()
	defer Mu.Unlock()
	ret := c.Status == Exit // 状态码可能被多协程修改，需要加锁保护
	log.Println("c.Status is", c.Status)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("MakeCoordinator called with files:", files, "and nReduce:", nReduce)
	numFiles = len(files)
	c := Coordinator{
		NReduce:          nReduce,
		nMap:             nReduce,
		WorkerId:         0,
		Workers:          make(map[int]struct{}),
		Tasks:            make(map[int]Task),
		nTask:            0,
		Status:           Wait,
		heartBeatChannel: make(chan bool, 2*nReduce),
		TasksQueue:       make(chan Task, max(nReduce, 2*numFiles)),
	}

	log.Println("Coordinator  created:", c)

	// 添加日志确认状态
	log.Printf("Before creating map tasks: nReduce: %d\n", nReduce)

	var TaskId int = 0
	// 对文件进行切分
	c.createMapTask(files, TaskId)

	// 启动rpc服务
	c.server()
	c.Status = Active // 状态改为Active
	log.Println("Coordinator status set to Active.")
	// 会内存逃逸
	return &c
}

// 已经将任务分割好了，内容存在了Input里
func (c *Coordinator) createMapTask(files []string, TaskId int) {
	// 添加日志确认
	log.Printf("Creating map tasks for files: %v\n", files)
	for _, file := range files {
		fileinfo, err := os.Stat(file)
		if err != nil {
			log.Fatalf("无法获取文件%s的信息:%v", file, err)
		}
		filesize := fileinfo.Size()
		fileNums := math.Ceil(float64(filesize) / float64(minSplitSize)) // 计算需要切分的文件数

		for i := 0; i < int(fileNums); i++ {
			start := int64(i * minSplitSize)
			end := start + minSplitSize

			if end > filesize {
				end = filesize
			}

			filess, err := os.OpenFile(file, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatalf("无法打开文件%s:%v", files, err)
			}

			defer filess.Close()

			_, err0 := filess.Seek(start, io.SeekStart)

			if err0 != nil {
				fmt.Println("seek error:", err)
			}

			input := make([]byte, end-start)

			n, err1 := filess.Read(input)

			if err1 != nil {
				fmt.Println("read error:", err)
			}

			task := Task{
				Id:         TaskId,
				Input:      string(input[:n]),
				NReduce:    c.NReduce,
				Tasktype:   Map,
				Taskstatus: TaskPending,
				MiddleData: nil,
				Output:     "",
			}
			log.Printf("Creating task for file: %s, Task ID: %d\n", file, TaskId)

			Mu.Lock()
			c.Tasks[TaskId] = task // 写入任务数组
			c.nTask++
			TaskId++
			c.TasksQueue <- task // 写入Map任务队列
			Mu.Unlock()
		}
		log.Printf("Map tasks created, total tasks: %d\n", TaskId)
	}
}

/*
	没必要使用单例模式了，因为多进程下内存空间独立
*/
// // 获取 Coordinator 实例
// func GetInstance() *Coordinator {
// 	once.Do(func() {
// 		instance = &Coordinator{
// 			NReduce:          numReduce,
// 			nMap:             numReduce,
// 			WorkerId:         0,
// 			Workers:          make(map[int]struct{}),
// 			Tasks:            make(map[int]Task),
// 			nTask:            0,
// 			Status:           Wait,
// 			heartBeatChannel: make(chan bool, 2*numReduce),
// 			TasksQueue:       make(chan Task, max(numReduce, 2*numFiles)),
// 		}
// 		log.Println("Coordinator instance created:", instance)
// 	})
// 	return instance
// }

func (c *Coordinator) createReduceTask(task *Task) {
	log.Println("createReduceTask start")
	c.TasksQueue <- *task
}
