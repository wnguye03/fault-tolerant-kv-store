package kvraft

import (
	"lab5/labgob"
	"lab5/labrpc"
	"lab5/logger"
	"lab5/raft"
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

	results      map[int]chan Op
	maxraftstate int
	// Hashmap for ongoing calls
	// index --> channel
	notifyCh map[int]chan Result

	//duplicate detection
	lastApplied map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// defer kv.mu.Unlock()

	getOperation := Op{
		Operation: "Get",
		Key:       args.Key,
	}

	index, _, isLeader := kv.rf.Start(getOperation)

	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	//create new entry here in hashmap
	channel := make(chan Result, 1)
	kv.notifyCh[index] = channel
	kv.mu.Unlock()
	// timeouts and seperate go routine

	//wait for res/timeout
	select {
	case result := <-channel:
		reply.Value = result.Value
		reply.Err = result.Err

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

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
	// Your code here.
	kv.mu.Lock()

	if lastReq, ok := kv.lastApplied[args.ClerkId]; ok && lastReq >= args.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	kv.results[index] = ch
	delete(kv.results, index)
	kv.mu.Unlock()
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// logger := Logger(me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	//background loop waiting for applyMSG
	go func() {
		for {
			applyMsg := <-kv.applyCh

			// if applyMsg.CommandIndex

			if applyMsg.CommandValid {
				kv.mu.Lock()

				//cast to Op type
				operation := applyMsg.Command.(Op)

				//swithc on Operation
				if operation.Operation == "Get" {
					value, keyFound := kv.db[operation.Key]

					//get the matching waiting thread
					channel, _ := kv.notifyCh[applyMsg.CommandIndex]

					//notify waiting thread
					if keyFound {
						channel <- Result{
							Value: value,
							Err:   OK,
						}
					} else {
						channel <- Result{
							Value: "",
							Err:   ErrNoKey,
						}
					}
				} else if operation.Operation == "Put" {

				} else if operation.Operation == "Append" {

				}

				//remove entry
				delete(kv.notifyCh, applyMsg.CommandIndex)
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
