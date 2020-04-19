package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// Reduce的过程如下：
	//  S1: 获取到Map产生的文件并打开(reduceName获取文件名)
	// 　S2：获取中间文件的数据(对多个map产生的文件更加值合并)
	// 　S3：打开文件（mergeName获取文件名），将用于存储Reduce任务的结果
	// 　S4：合并结果之后(S2)，进行reduceF操作, work count的操作将结果累加，也就是word出现在这个文件中出现的次数

	datas := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inFileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.Open(inFileName)
		if err != nil {
			fmt.Printf("failed to open %s:%v", inFileName, err)
			return
		}

		var kv KeyValue
		dec := json.NewDecoder(inFile)
		for dec.Decode(&kv) == nil {
			data, ok := datas[kv.Key]
			if !ok {
				data = []string{}
			}

			data = append(data, kv.Value)
			datas[kv.Key] = data
		}

		inFile.Close()
	}

	outFileName := mergeName(jobName, reduceTaskNumber)
	outFile, err := os.Create(outFileName)
	if err != nil {
		fmt.Printf("failed to create %s:%v", outFileName, err)
		return
	}
	defer outFile.Close()

	enc := json.NewEncoder(outFile)
	for key, values := range datas {
		res := reduceF(key, values)
		err := enc.Encode(KeyValue{Key: key, Value: res})
		if err != nil {
			fmt.Printf("failed to encdoe:%v\n", err)
			return
		}
	}
}
