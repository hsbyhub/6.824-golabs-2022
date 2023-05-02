package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ReqMapArgs struct {
	Worker int
}

type ReqMapReply struct {
	FileName   string
	BuketCount int
}

type RspMapArgs struct {
	Worker           int
	FileName         string
	BuketFileNameMap map[int]string
	Err              error
}

type RspMapReply struct {
}

type ReqReduceArgs struct {
	Worker int
}

type ReqReduceReply struct {
	Buket     int
	FileNames []string
}

type RspReduceArgs struct {
	Worker int
	Buket  int
	Err    error
}

type RspReduceReply struct {
}

type IsDoneArgs struct {
}

type IsDoneReply struct {
	IsDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/824-mr-%d", os.Getuid())
}
