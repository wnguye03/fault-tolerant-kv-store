package kvraft

import (
	"crypto/rand"
	"time"

	"kvraft/constants"
	"kvraft/rpc"
	"kvraft/logger"
	"math/big"
)

type Clerk struct {
	servers []*rpc.ClientEnd
	logger  *logger.Logger
	clerkId   int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	//println("MakeClerk")
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.requestId = 0
	ck.logger = logger.NewLogger(int(ck.clerkId), true, "Clerk", constants.ClerkLoggingMap)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// ck.mu.Lock()
	ck.requestId++


	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}
	// ck.mu.Unlock()
	//println("c get")
	// try forever
	for {
		//For every server
		for _, server := range ck.servers {
			reply := GetReply{}

			//Try to get
			ok := server.Call("KVServer.Get", &args, &reply)

			//handle result
			if ok && reply.Err == OK {
				//println("c get ok")
				return reply.Value
			}
			if ok && reply.Err == ErrNoKey {
				//println("c get no key")
				return ""
			}

		}
		time.Sleep(100 * time.Millisecond)

		// result := ""
		// for _, server := range ck.servers {
		// 	args := GetArgs{
		// 		Key: key,
		// 	}
		// 	reply := GetReply{}
		// 	ok := server.Call("KVServer.Get", &args, &reply)

		// 	if ok {
		// 		result = reply.Value
		// 		break
		// 	}
		// }
		// // ok := ck.servers[i]
		// // You will have to modify this function.
		// return result
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//println("c putappend")
	// You will have to modify this function.
	// ck.mu.Lock()
	ck.requestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}
	// ck.mu.Unlock()

	for {
		//println("c putappend 0")
		for _, server := range ck.servers {
			//println("c putappend 1")
			reply := PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", &args, &reply)

			if ok && reply.Err == OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	//println("c put")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//println("c append")
	ck.PutAppend(key, value, "Append")
}
