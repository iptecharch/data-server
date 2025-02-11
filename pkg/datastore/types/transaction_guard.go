package types

type TransactionGuard struct {
	cleanup func()
}

// NewTransactionGuard initializes the guard and ensures cleanup on function exit.
func NewTransactionGuard(cleanup func()) *TransactionGuard {
	tg := &TransactionGuard{cleanup: cleanup}
	return tg
}

// Success prevents the cleanup function from being called.
func (tg *TransactionGuard) Success() {
	tg.cleanup = func() {} // Set to no-op
}

// Done Ensure cleanup runs when the guard goes out of scope.
func (tg *TransactionGuard) Done() {
	if tg.cleanup != nil {
		tg.cleanup()
	}
}
