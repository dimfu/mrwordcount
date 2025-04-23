package shared

type TaskType int

type Args struct {
	ID      string
	Host    string
	Port    int
	Task    TaskType
	Payload []byte
}

type Reply struct {
	Message string
}

type TimeServer int64

const (
	TASK_MAP TaskType = iota
	TASK_REDUCE
	TASK_UNDEFINED
)

const (
	PROCESSING = "PROCESSING"
	FINISH     = "FINISH"
	IDLE       = "IDLE"
)
