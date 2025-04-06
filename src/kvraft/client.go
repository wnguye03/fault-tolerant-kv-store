package kvraft

import (
	"crypto/rand"
	"time"

	// "lab5/constants"
	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *logger.Logger
	// You will have to modify this struct.
	clerkId   int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	println("MakeClerk")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
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
	println("c get")
	// try forever
	for {
		//For every server
		for _, server := range ck.servers {

			args := GetArgs{
				Key: key,
			}
			reply := GetReply{}

			//Try to get
			ok := server.Call("KVServer.Get", &args, &reply)

			//handle result
			if ok {
				if reply.Err == OK {
					println("c get ok")
					return reply.Value
				} else if reply.Err == ErrNoKey {
					println("c get no key")
					return ""
				} else if reply.Err == ErrWrongLeader {
					println("c get wrong leader")
					continue
				}
			}
		}

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
	println("c putappend")
	// You will have to modify this function.
	ck.requestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}

	for {
		println("c putappend 0")
		for i := 0; i < len(ck.servers); i++ {
			println("c putappend 1")
			reply := PutAppendReply{}

			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					println("c putappend ok")
					return
				} else {
					println("c putappend not ok")
					continue
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	println("c put")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	println("c append")
	ck.PutAppend(key, value, "Append")
}
