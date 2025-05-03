package kvraft

import (
	"kvraft/gob"
	"kvraft/rpc"
	"kvraft/logger"
	"kvraft/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string //get/append/put
	Key       string
	Value     string
	ClerkId   int64
	RequestId int64
}

type Result struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	logger *logger.Logger
	// Your definitions here.

	db map[string]string

	maxraftstate int
	// Hashmap for ongoing calls
	// index --> channel
	notifyCh map[int]chan Result

	//duplicate detection
	lastApplied map[int64]int
}

func (kv *KVServer) sendOp(op Op) (Result, bool) {
	//kv.mu.Lock()
	//println("sendop 1")
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		//kv.mu.Unlock()
		return Result{Err: ErrWrongLeader}, false
	}

	//println("sendop 2")

	kv.mu.Lock()
	ch := make(chan Result, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	//println("sendop 3")

	select {
	case result := <-ch:
		return result, true
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
		return Result{Err: ErrTimeout}, false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//println("s get")
	// Your code here.

	// defer kv.mu.Unlock()
	//println("s get 1")
	
	getOperation := Op{
		Operation: "Get",
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}

	result, ok := kv.sendOp(getOperation)
	if !ok {
		reply.Err = result.Err
		return
	}

	reply.Value = result.Value
	reply.Err = result.Err

	//println("s get 3")

	// WE HAVE 2 seperate threads
	// 1 thread polling the apply channel for all messages
	// if a message matches something in our hashmap of ongoing calls notify get that there is a message for you

	// on main thread wait for message (blocking) then once recieves response parse response then return to client

	//place inide thread
	// key := args.Key
	// value, exists := kv.db[key]

	// if !exists {
	// 	reply.Value = ""
	// } else {
	// 	reply.Value = value
	// }

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//println("s putappend")
	// Your code here.
	//kv.mu.Lock()
	//println("s putappend 0")

	if lastReq, ok := kv.lastApplied[args.ClerkId]; ok && lastReq >= int(args.RequestId) {
		//println("s putappend 1")
		reply.Err = OK
		//kv.mu.Unlock()
		return
	}
	// result := Result{
	// 	Err:   "OK",
	// 	Value: args.Value,
	// }

	// getOperation := Op{
	// 	Operation: "Get",
	// 	Key:       args.Key,
	// }
	operation := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	//println("s putappend 2")

	result, ok := kv.sendOp(operation)
	if !ok {
		//println("s putappend 3")
		reply.Err = result.Err
		return
	}

	//println("s putappend 4")

	reply.Err = result.Err

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*rpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	//println("s start")
	// logger := Logger(me)
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.notifyCh = make(map[int]chan Result)
	kv.lastApplied = make(map[int64]int)

	//background loop waiting for applyMSG
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh

			// if applyMsg.CommandIndex

			if applyMsg.CommandValid {
				kv.mu.Lock()

				//cast to Op type
				operation := applyMsg.Command.(Op)

				var result Result

				//if statement on Operation
				//get the matching waiting thread
				if operation.Operation == "Get" {
					value, keyFound := kv.db[operation.Key]
					if keyFound {
						result = Result{Value: value, Err: OK}
					} else {
						result = Result{Value: "", Err: ErrNoKey}
					}
				} else if operation.Operation == "Put" {
					if lastReq, exists := kv.lastApplied[operation.ClerkId]; !exists || lastReq < int(operation.RequestId) {
						kv.db[operation.Key] = operation.Value
						kv.lastApplied[operation.ClerkId] = int(operation.RequestId)
					}
					result = Result{Err: OK}
				} else if operation.Operation == "Append" {
					if lastReq, exists := kv.lastApplied[operation.ClerkId]; !exists || lastReq < int(operation.RequestId) {
						currentValue, ok := kv.db[operation.Key]
						if ok {
							kv.db[operation.Key] = currentValue + operation.Value
						} else {
							kv.db[operation.Key] = operation.Value
						}
						kv.lastApplied[operation.ClerkId] = int(operation.RequestId)
					}
					result = Result{Err: OK}
				}

				if ch, ok := kv.notifyCh[applyMsg.CommandIndex]; ok {
					ch <- result
					//remove entry
					delete(kv.notifyCh, applyMsg.CommandIndex)
				}

				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
