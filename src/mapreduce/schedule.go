package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)

	// 使用WaitGroup来同步
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		go func(taskNum int, nios int, phase jobPhase) {
			for {
				worker := <-mr.registerChannel // 尝试获取worker资源
				fmt.Printf("current worker port: %v\n", worker)

				var args DoTaskArgs
				args.JobName = mr.jobName
				args.File = mr.files[taskNum] // 只在map中使用，表明map任务需要处理哪一个文件
				args.Phase = phase            // 标记是map任务还是reduce任务
				args.TaskNumber = taskNum     // 任务的编号
				args.NumOtherPhase = nios     // 如果是map任务，表示为reduce分区数，如果是reduce任务，表示为map任务的个数

				ok := call(worker, "Worker.DoTask", &args, new(struct{})) // rpc调用worker处理任务
				if ok {
					wg.Done()                    // 完成
					mr.registerChannel <- worker // 归还worker资源
					break
				}

				// 如果不成功就重试
				fmt.Printf("task#%d failure\n", taskNum)
			}
		}(i, nios, phase)
	}

	wg.Wait()
}
