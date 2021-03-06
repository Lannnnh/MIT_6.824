package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

type TryMapArgs struct {
}

type TryMapReply struct {
	// if should not run map, so should run reduce
	RunMap bool
}

const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type TaskInfo struct {
	State int

	FileName  string
	FileIndex int
	PartIndex int

	NReduce int
	NFiles  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
