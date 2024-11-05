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
	"time"
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
	TaskPending    TaskStatus = iota // 任务等待处理
	TaskInProgress                   // 任务正在处理
	Completed                        // 任务处理完成
)

type TaskType int

const (
	Map    TaskType = iota // Map任务
	Reduce                 // Reduce任务
	Wait                   // 等待任务
	Exit                   // 通知Worker退出
)

type Task struct {
	Id            int        // 用来标识一个任务
	NReduce       int        // 用来存储Reduce任务的个数 （由于运行在不同进程使用全局变量行不通了，当然也可以使用PRC来传递此信息）
	InputFileName string     // 输入文件的名称
	Input         string     // 输入分割后的内容
	Tasktype      TaskType   // 用来存储任务的类型
	Taskstatus    TaskStatus // 用来存储任务的状态
	TaskStartTime time.Time  // 任务开始时间(用于看是否超时)
	MiddleData    []string   // 用来存储中间结果被存放在哪个文件的路径
	Output        string     // 处理后输出的结果
}

type Coordinator struct {
	// Your definitions here.
	NReduce       int           // 用来存储Reduce任务的个数
	Status        TaskType      // 用来存储Coordinator的阶段
	Tasks         map[int]*Task // 用来存储任务的数组
	nTask         int           // 用来存储任务的个数
	Intermediates [][]string    // Map任务产生的R个中间文件的信息

	TasksQueue chan Task // 用来给Worker发送任务
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

	//log.Println("AssignTask called, len(TasksQueue):", len(c.TasksQueue))

	if len(c.TasksQueue) > 0 {
		// 任务队列不为空，取出一个任务
		*reply = <-c.TasksQueue
		// 记录此次任务的开始时间
		reply.TaskStartTime = time.Now()
		reply.Taskstatus = TaskInProgress // 任务状态改为TaskInProgress
		c.Tasks[reply.Id] = reply         // 更新任务数组

	} else if c.Status == Exit {
		// 提示Worker退出
		*reply = Task{Tasktype: Exit}
	} else {
		// 提示Worker等待任务
		*reply = Task{Tasktype: Wait}
	}

	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *NullReply) error {
	Mu.Lock()
	defer Mu.Unlock()
	//log.Println("TaskComplated called, task.Id:", task.Id)
	if c.Tasks[task.Id].Taskstatus == Completed || task.Tasktype != c.Status { // 如果是重复完成任务或者任务类型和当前阶段不匹配，则忽略
		return nil
	}
	c.Tasks[task.Id].Taskstatus = Completed // 任务状态改为Completed
	go c.processTaskResult(task)
	return nil
}

// 处理任务结果的函数
func (c *Coordinator) processTaskResult(task *Task) {
	Mu.Lock()
	defer Mu.Unlock()
	//log.Println("processTaskResult called, task.Id:", task.Id)
	switch task.Tasktype {
	case Map:
		// 首先将任务的中间结果存入 Intermediates 中
		for idx, filename := range task.MiddleData {
			c.Intermediates[idx] = append(c.Intermediates[idx], filename)
		}
		//c.Intermediates[task.Id] = task.MiddleData
		// 然后检查是否所有Map任务都完成
		if c.CheckAllTaskDone() {
			// 所有Map任务都完成，创建Reduce任务
			c.createReduceTask()
			c.Status = Reduce // 状态改为Reduce
		}
	case Reduce:
		if c.CheckAllTaskDone() {
			// 所有Reduce任务都完成，通知Worker退出
			c.Status = Exit
		}
	}
}

func (c *Coordinator) CheckAllTaskDone() bool {
	// 遍历任务数组，检查是否所有任务都完成
	flag := true
	for _, task := range c.Tasks {
		if task.Taskstatus != Completed {
			flag = false
			break
		}
	}
	return flag
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	//log.Println("RPC server started.")
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

// 用来检查任务是否超时，这就可以做到心跳检测
func (c *Coordinator) CheckTaskTimeout() {
	for {
		time.Sleep(time.Second * 5) // 5秒检测一次
		Mu.Lock()
		// 如果为退出状态，就直接放回
		if c.Status == Exit {
			Mu.Unlock()
			return
		}
		for _, task := range c.Tasks {
			if task.Taskstatus == TaskInProgress && time.Now().Sub(task.TaskStartTime) > time.Second*10 {
				// 任务处理时间超过10s，认为超时（可能worker宕机了），重新分配任务
				//log.Printf("Task %d timeout, reassigning task.\n", task.Id)
				task.Taskstatus = TaskPending // 任务状态改为TaskPending
				c.TasksQueue <- *task         // 重新将任务放入任务队列中
			}
		}
		Mu.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	Mu.Lock()
	defer Mu.Unlock()
	ret := c.Status == Exit // 状态码可能被多协程修改，需要加锁保护
	//log.Println("c.Status is", c.Status)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//log.Println("MakeCoordinator called with files:", files, "and nReduce:", nReduce)
	numFiles = len(files)
	c := Coordinator{
		NReduce:       nReduce,
		Tasks:         make(map[int]*Task),
		nTask:         0,
		Status:        Map,
		Intermediates: make([][]string, nReduce),
		TasksQueue:    make(chan Task, max(nReduce, 2*numFiles)),
	}

	//log.Println("Coordinator  created:", c)

	// 添加日志确认状态
	//log.Printf("Before creating map tasks: nReduce: %d\n", nReduce)

	var TaskId int = 0
	// 对文件进行切分
	c.createMapTask(files, TaskId)

	// 启动rpc服务
	c.server()
	//log.Println("Coordinator status set to Active.")

	go c.CheckTaskTimeout() // 启动任务超时检测线程
	// 会内存逃逸
	return &c
}

// 已经将任务分割好了，内容存在了Input里
func (c *Coordinator) createMapTask(files []string, TaskId int) {
	// 添加日志确认
	//log.Printf("Creating map tasks for files: %v\n", files)
	// 遍历每个文件，获取文件大小，计算需要切分的文件数，创建Map任务
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

			task := &Task{
				Id:            TaskId,
				InputFileName: file,
				Input:         string(input[:n]),
				NReduce:       c.NReduce,
				Tasktype:      Map,
				Taskstatus:    TaskPending,
				MiddleData:    nil,
				Output:        "",
			}
			//log.Printf("Creating task for file: %s, Task ID: %d\n", file, TaskId)

			Mu.Lock()
			c.Tasks[TaskId] = task // 写入任务数组
			c.nTask++
			TaskId++
			c.TasksQueue <- *task // 写入Map任务队列
			Mu.Unlock()
		}
		//log.Printf("Map tasks created, total tasks: %d\n", TaskId)
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
// 		//log.Println("Coordinator instance created:", instance)
// 	})
// 	return instance
// }

func (c *Coordinator) createReduceTask() {
	//log.Println("createReduceTask start")
	// 遍历 Intermediates，获取每个Map任务产生的Reduce个中间文件的信息
	for idx, files := range c.Intermediates {
		// 创建Reduce任务
		task := &Task{
			Id:         idx,
			Input:      "",
			NReduce:    c.NReduce,
			Tasktype:   Reduce,
			Taskstatus: TaskPending,
			MiddleData: files,
			Output:     "",
		}
		//log.Printf("Creating task for reduce, Task ID: %d\n", idx)
		c.Tasks[idx] = task   // 写入任务数组
		c.TasksQueue <- *task // 写入Reduce任务队列
	}
}
