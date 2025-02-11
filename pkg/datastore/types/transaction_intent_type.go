package types

type TransactionIntentType int

const (
	TransactionIntentNew TransactionIntentType = iota // Starts at 0
	TransactionIntentOld                              // Incremented to 1
)
