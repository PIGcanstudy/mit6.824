package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (k ByKey) Len() int           { return len(k) }
func (k ByKey) Less(i, j int) bool { return k[i].Key < k[j].Key }
func (k ByKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] } // 因为是切片所以底层有引用，可以改变切片现有值

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 把中间结果保存到了磁盘中,存入中间结果的名为task_id.txt
func saveTaskToLocal(x int, y int, kva []KeyValue) string {

	log.Println("saveTaskToLocal Start")

	dir, _ := os.Getwd() // 获取当前的文件目录路径

	file, err := os.CreateTemp(dir, "mr-tmp-*")

	if err != nil {
		log.Fatal("Faild To Create TempFile", err)
	}

	enc := json.NewEncoder(file) // 创建序列化器

	for _, kv := range kva {
		err := enc.Encode(kv)
		if err != nil {
			log.Fatal("Faild To Encode KeyValue", err)
		}
	}

	file.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(file.Name(), outputName)    // 重命名文件
	return filepath.Join(dir, outputName) //返回中间结果文件的存储路径
}

// 从本地磁盘读取中间结果文件，返回一个KeyValue切片
func readFromLocalFile(filename []string) *[]KeyValue {
	var kva []KeyValue
	for _, name := range filename {
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("Faild To Open File", err)
		}
		defer file.Close()
		dec := json.NewDecoder(file) // 创建反序列化器
		for {
			var tmp KeyValue
			err := dec.Decode(&tmp)
			if err == io.EOF {
				break // 到达文件末尾，退出循环
			}
			if err != nil {
				log.Fatal("失败读取 KeyValue", err) // 处理读取错误
			}
			kva = append(kva, tmp) // 将读取到的 KeyValue 添加到切片
		}

	}
	return &kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here. 你需要实现的worker函数。
	// 循环获取任务
	for {
		// 从协调器的任务管道中接受任务
		task := getTask()
		switch task.Tasktype {
		case Map:
			// 调用mapper函数处理任务
			mapper(&task, mapf)
		case Reduce:
			// 调用reducer函数处理任务
			reducer(&task, reducef)
		case Exit:
			// 退出worker
			return
		case Wait:
			time.Sleep(5 * time.Second) // 等待5秒
			// 等待有任务到来
		default:
			// 未知任务类型
			fmt.Println("Unknown task type: ", task.Tasktype)
		}
	}
}

func reducer(task *Task, reducef func(string, []string) string) {

	log.Println("reducer start: Id is ", task.Id)

	// 读取中间结果文件
	kva := *readFromLocalFile(task.MiddleData)

	// 按key给kva排序
	sort.Sort(ByKey(kva))

	// 定义临时文件
	dir, _ := os.Getwd() // 获取当前的文件目录路径
	tmpfile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// 将有相同key的合并成一个
	for i := 0; i < len(kva)-1; i++ {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}

		var values []string // 用来存储合并的value值

		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values) // 统计后的数量

		//写到对应的output文件
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j - 1
	}
	// 关闭文件
	tmpfile.Close()
	// 重命名文件
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tmpfile.Name(), oname)

	task.Output = filepath.Join(dir, oname) // 记录输出文件的路径

	log.Println("reducer end: Id is ", task.Id)

	ReducenotifyDone(task)
}

// 通知协调器任务完成所作的操作（告知协调器中间结果文件路径）
func ReducenotifyDone(Task *Task) {
	reply := NullReply{}
	call("Coordinator.ReduceTaskComplated", Task, &reply) // 调用协调器的ComplateTask函数，通知任务完成
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {

	log.Println("开始执行mapper")

	taskName := strconv.Itoa(task.Id)
	// 调用map函数处理任务
	kva := mapf(taskName, task.Input)

	// 清空输入的数据
	task.Input = ""

	// 将中间结果分成nReduce份，并保存在磁盘中
	// 切分方式可以根据math.Ceil(float64(len(kva)) / float64(nReduce))来决定每个里面存放多少个
	// 这里采用一位大佬的切分方式，把key进行hash，然后对nReduce取模，把key-value对存入对应的reduce任务的中间结果文件中

	buffer := make([][]KeyValue, task.NReduce) // 定义存放分成nReduce份的中间结果的buffer

	log.Println("task.NReduce: ", task.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		buffer[idx] = append(buffer[idx], kv)
	}

	AddrOutPut := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		AddrOutPut = append(AddrOutPut, saveTaskToLocal(task.Id, i, buffer[i]))
	}

	task.MiddleData = append(task.MiddleData, AddrOutPut...) // 将中间结果文件路径添加到task.MiddleData中

	// 通知协调器任务完成所作的操作（告知协调器中间结果文件路径）
	mapnotifyDone(task)
}

func mapnotifyDone(task *Task) {
	reply := NullReply{}
	call("Coordinator.MapTaskComplated", task, &reply) // 调用协调器的ComplateTask函数，通知任务完成
}

// 可以有两种实现方式，一种是让协调器为单例模式，然后获取协调器单例，并从单例中的任务管道获取数据（此方法在多进程不适用）
// 另一种方式就是使用RPC调用的方式，让worker直接调用协调器的函数，获取任务数据。
// 这里使用第二中方法来实现
func getTask() Task {
	// 请求参数为空，因为没必要传参
	args := NullArgs{}
	reply := Task{}

	// 发送RPC请求并等待响应
	call("Coordinator.AssignTask", &args, &reply)

	log.Printf("获取到了的任务是：%v\n", reply)

	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

// 如果每台机器上运行这不同的worker，同时每台机器都有着自己的磁盘，所以要解决一个Worker如何调用另一个Worker的问题
