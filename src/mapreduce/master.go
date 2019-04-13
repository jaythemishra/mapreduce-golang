package mapreduce

import "container/list"
import "fmt"
import "sync"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

//RunMaster assigns Map and Reduce jobs to available workers
func (mr *MapReduce) RunMaster() *list.List {
	AssignJob := func(jobNumber int, operation string, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			worker := <-mr.registerChannel
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = JobType(operation)
			args.JobNumber = jobNumber
			if operation == Map {
				args.NumOtherPhase = mr.nReduce
			} else if operation == Reduce {
				args.NumOtherPhase = mr.nMap
			}
			var reply DoJobReply
			ok := call(worker, "Worker.DoJob", args, &reply)
			if ok == true {
				go func() {
					mr.registerChannel <- worker
				}()
				break
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < mr.nMap; i++ {
		wg.Add(1)
		go AssignJob(i, Map, &wg)
	}
	wg.Wait()
	for i := 0; i < mr.nReduce; i++ {
		wg.Add(1)
		go AssignJob(i, Reduce, &wg)
	}
	wg.Wait()
	return mr.KillWorkers()
}
