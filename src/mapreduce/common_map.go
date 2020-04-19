package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
// doMap是一个map worker的工作，它读入一个输入文件，然后调用用户自定义的map函数，最后将结果输出到nReduce个文件中
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string, // 处理的输入文件
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	// 1.读取输入文件的所有内容
	// 2.调用map函数处理成kv
	// 3.对kv进行分类
	// 4.将分好类的kv写入到对应分区的文件中

	// 1.读取输入文件的所有内容
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Printf("failed to open %s: %v", inFile, err)
		return
	}

	// 2.调用map函数处理成kv
	kvs := mapF(inFile, string(contents))

	// 3.将所有的kv进行分类
	datas := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % uint32(nReduce)
		datas[index] = append(datas[index], kv)
	}

	// 4.将数据写入对应的分区文件中
	for nr, data := range datas {
		outFileName := reduceName(jobName, mapTaskNumber, nr)
		outFile, err := os.Create(outFileName)
		if err != nil {
			fmt.Printf("failed to create %s", outFileName)
			return
		}

		enc := json.NewEncoder(outFile)
		for _, kv := range data {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("failed to encode:%v\n", err)
				outFile.Close()
				return
			}
		}
		outFile.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
