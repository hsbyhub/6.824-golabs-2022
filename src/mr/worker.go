package mr

import (
	"6.824/util/logs"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := os.Getpid()
	for {
		// map
		var reqMapArgs = GetMapReq{Worker: worker}
		var reqMapReply GetMapRsp
		err := callCoordinator("Coordinator.OnGetMap", &reqMapArgs, &reqMapReply)
		if err != nil {
			break
		}
		if len(reqMapReply.FileName) > 0 {
			mapWork(worker, reqMapReply.FileName, reqMapReply.BuketCount, mapf)
		}

		// reduce
		var reqReduceArgs = GetReduceReq{Worker: worker}
		var reqReduceReply GetReduceRsp
		err = callCoordinator("Coordinator.OnGetReduce", &reqReduceArgs, &reqReduceReply)
		if err != nil {
			break
		}
		if len(reqReduceReply.FileNames) > 0 {
			reduceWork(worker, reqReduceReply.Buket, reqReduceReply.FileNames, reducef)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapWork(worker int, fileName string, buketCount int, mapFunc func(string, string) []KeyValue) {
	logs.Info("worker[%v] begin map work", worker)
	kvs, err := mapFile(worker, fileName, buketCount, mapFunc)
	if err != nil {
		logs.Error("worker[%v] map file error[%v]", worker, err)
		postMap(worker, fileName, err, nil)
		return
	}

	buketFileNameMap := map[int]string{}
	buketFileMap := map[int]*os.File{}
	for _, kv := range kvs {
		buket := ihash(kv.Key) % buketCount

		_, fileName := filepath.Split(fileName)
		buketFileName := fmt.Sprintf("mr-buket-%s-%d", fileName, buket)
		if _, ok := buketFileMap[buket]; !ok {
			var buketFile *os.File
			buketFile, err = os.Create(buketFileName)
			if err != nil {
				break
			}
			buketFileNameMap[buket] = buketFileName
			buketFileMap[buket] = buketFile
		}

		fmt.Fprintf(buketFileMap[buket], "%v %v\n", kv.Key, kv.Value)
	}

	if !postMap(worker, fileName, err, buketFileNameMap) {
		logs.Error("worker[%v] post map file[%v] fail", worker, fileName)

		for _, v := range buketFileNameMap {
			err := os.Remove(v)
			if err != nil {
				logs.Error("worker[%v] remove buket file[%v] error[%v]", worker, v, err)
			}
		}
	}
}

func mapFile(worker int, fileName string, buketCount int, mapFunc func(string, string) []KeyValue) ([]KeyValue, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return mapFunc(fileName, string(content)), nil
}

func postMap(worker int, fileName string, err error, buketFileNameMap map[int]string) bool {
	var rspMapArgs = PostMapReq{
		Worker:           worker,
		FileName:         fileName,
		Err:              err,
		BuketFileNameMap: buketFileNameMap,
	}
	var rspMapReply PostMapRsp
	err = callCoordinator("Coordinator.OnPostMap", &rspMapArgs, &rspMapReply)
	if err != nil || !rspMapReply.OK {
		return false
	}
	return true
}

func reduceWork(worker int, buket int, fileNames []string, reduceFunc func(string, []string) string) {
	content, err := reduceFile(worker, buket, fileNames, reduceFunc)
	if err != nil {
		logs.Error("worker[%v] reduce buket[%v] error[%v]", worker, buket, err)
	}

	outFileName := fmt.Sprintf("mr-out-%d", buket)
	outFile, err := os.Create(outFileName)
	if err != nil {
		return
	}
	defer outFile.Close()
	fmt.Fprint(outFile, content)

	if postReduce(worker, buket, err) {
		for _, v := range fileNames {
			os.Remove(v)
		}
	} else {
		logs.Error("worker[%v] post reduce buket[%v] fail", worker, buket)
		os.Remove(outFileName)
	}
}

func reduceFile(worker int, buket int, fileNames []string, reduceFunc func(string, []string) string) (string, error) {
	kvs := ByKey{}
	for _, v := range fileNames {
		inFile, err := os.Open(v)
		if err != nil {
			return "", err
		}

		for {
			var kv KeyValue
			n, err := fmt.Fscanf(inFile, "%s %s\n", &kv.Key, &kv.Value)
			if err != nil {
				if err != io.EOF {
					return "", err
				}
				break
			}
			if n != 2 {
				return "", fmt.Errorf("reduce file[%v] scanf count not 2", worker, v)
			}
			kvs = append(kvs, kv)
		}

		inFile.Close()
	}
	sort.Sort(kvs)

	var content string
	var curKey string
	var curVs = []string{}
	for _, kv := range kvs {
		if kv.Key != curKey {
			if len(curVs) > 0 {
				content += fmt.Sprintf("%v %v\n", curKey, reduceFunc(curKey, curVs))
			}
			curKey = kv.Key
			curVs = []string{}
		}
		curVs = append(curVs, kv.Value)
	}
	if len(curVs) > 0 {
		content += fmt.Sprintf("%v %v\n", curKey, reduceFunc(curKey, curVs))
	}
	return content, nil
}

func postReduce(worker int, buket int, err error) bool {
	var rspReduceArgs = PostReduceReq{
		Worker: worker,
		Buket:  buket,
		Err:    err,
	}
	var rspReduceReply PostReduceRsp
	err = callCoordinator("Coordinator.OnPostReduce", &rspReduceArgs, &rspReduceReply)
	if err != nil || !rspReduceReply.OK {
		return false
	}
	return true
}

// example function to show how to make an RPC callCoordinator to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to callCoordinator
	// the Example() method of struct Coordinator.
	err := callCoordinator("Coordinator.Example", &args, &reply)
	if err != nil {
		fmt.Printf("callCoordinator failed!\n")
	} else {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func callCoordinator(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
