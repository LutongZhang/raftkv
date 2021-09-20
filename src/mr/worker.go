package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func readFile(filename string)[]byte{
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		requestTaskArgs := RequestTaskArgs{
			"worker",
			0,
		}
		requestTaskReturn := RequestTaskReturn{}
		success := call("Coordinator.RequestTask", &requestTaskArgs, &requestTaskReturn)
		if !success{
			fmt.Println("no success")
			continue
		}
		if requestTaskReturn.Task.TaskType == SLEEP{
			time.Sleep(3*time.Second)
		} else if requestTaskReturn.Task.TaskType == MAP{
			//fmt.Println("xx")
			fs,err := doMap(mapf,requestTaskReturn)
			if err != nil{
				continue
			}
			reportTaskArgs := ReportTaskArgs{
				requestTaskReturn.Task,
					fs,
			}
			reportTaskReturn := ReportTaskReturn{}
			success = call("Coordinator.ReportTask",&reportTaskArgs,&reportTaskReturn)
			if ! success{
				continue
			}
		} else if requestTaskReturn.Task.TaskType == REDUCE{
			//fmt.Println("xx")
			fs,err := doReduce(reducef,requestTaskReturn)
			if err != nil{
				continue
			}
			reportTaskArgs := ReportTaskArgs{
				requestTaskReturn.Task,
				fs,
			}
			reportTaskReturn := ReportTaskReturn{}
			success = call("Coordinator.ReportTask",&reportTaskArgs,&reportTaskReturn)
			if ! success{
				continue
			}
		}
	}
}

func doMap(mapf func(string, string) []KeyValue,requestTaskReturn RequestTaskReturn)([]string,error){
	kva := []KeyValue{}
	for _,fileName := range requestTaskReturn.Task.Input{
		content := readFile(fileName)
		val := mapf(fileName,string(content))
		kva = append(kva,val...)
	}
	X := requestTaskReturn.Task.TaskNumber
	files := make(map[string]*os.File)
	defer func(){
		for _,v := range files{
			v.Close()
		}
	}()
	for i := 0; i < requestTaskReturn.NReduce;i++{
		f,err := ioutil.TempFile(".","")
		if err != nil{
			return nil,errors.New("create file failed")
		}
		files[fmt.Sprintf("mr-%d-%d",X,i)]=f
	}
	for _, pair := range kva{
		Y := ihash(pair.Key) % requestTaskReturn.NReduce
		f := files[fmt.Sprintf("mr-%d-%d",X,Y)]
		enc := json.NewEncoder(f)
		err :=enc.Encode(&pair)
		if err != nil{
			return nil,err
		}
	}
	fs := []string{}
	for k,v := range files{
		err :=os.Rename(v.Name(),k)
		if err != nil{
			return nil,err
		}
		fs = append(fs,k)
	}
	return fs,nil
}


func doReduce(reducef func(string, []string) string,requestTaskReturn RequestTaskReturn)([]string,error){
	kva := []KeyValue{}
	files := make(map[string]*os.File)
	defer func(){
		for _,v := range files{
			v.Close()
		}
	}()
	for _,fileName := range requestTaskReturn.Task.Input{
		f, err := os.OpenFile(fileName,os.O_RDWR,os.ModePerm)
		if err != nil{
			return nil,err
		}
		files[fileName]=f
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d",requestTaskReturn.Task.TaskNumber)
	ofile, err := ioutil.TempFile(".","")
	if err != nil{
		return nil,err
	}
	defer func(){
		ofile.Close()
	}()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(),oname)
	return []string{oname},nil
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
