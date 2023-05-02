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
type GetMapReq struct {
	Worker int
}

type GetMapRsp struct {
	FileName   string
	BuketCount int
}

type PostMapReq struct {
	Worker           int
	FileName         string
	BuketFileNameMap map[int]string
	Err              error
}

type PostMapRsp struct {
	OK bool
}

type GetReduceReq struct {
	Worker int
}

type GetReduceRsp struct {
	Buket     int
	FileNames []string
}

type PostReduceReq struct {
	Worker int
	Buket  int
	Err    error
}

type PostReduceRsp struct {
	OK bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	return fmt.Sprintf("/var/tmp/824-mr-%d", os.Getuid())
}
