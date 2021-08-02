package domain

type Decision int32

const (
	Decision_COMMIT Decision = 0
	Decision_ABORT  Decision = 1
)

type Status int32

const (
	Ready 	Status = 0
	Commit 	Status = 1
	Abort 	Status = 2
)

type Entry struct {
	TxID  	string 	`json:"tx_id"`
	Key		string	`json:"key"`
	State 	Status 	`json:"state"`
	Len   	int	 	`json:"len"`
	Data  	[]byte 	`json:"data"`
}

type NotFoundError struct {
}

func (n NotFoundError) Error() string {
	return "Data not found!"
}

type AbortedError struct {

}

func (a AbortedError) Error() string {
	return "Transaction was aborted"
}


 
