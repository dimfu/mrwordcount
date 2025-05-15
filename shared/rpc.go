package shared

type TaskType int
type TaskStatus string

type Args struct {
	NReducer  int
	Host      string
	Port      int
	Task      TaskType
	Filename  string   // original text file name
	FileNames []string // to store collections of temp files
	Payload   []byte
}

type AckWorker struct {
	Ack      bool
	TaskType TaskType
}

type Reply struct {
	Message string
}

type TaskDone struct {
	FileNames []string
}

type TimeServer int64

type KV struct {
	Key   string
	Value int
}

const (
	TASK_MAP TaskType = iota
	TASK_REDUCE
	TASK_UNDEFINED
)

const (
	IN_PROGRESS TaskStatus = "IN_PROGRESS"
	FINISH                 = "FINISH"
	IDLE                   = "IDLE"
)
