package types

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	ErrTransactionOngoing error = errors.New("transaction ongoing")
)

type TransactionManager struct {
	tmMutex     *sync.Mutex
	transaction *Transaction
	rollbacker  RollbackInterface
}

func NewTransactionManager(r RollbackInterface) *TransactionManager {
	return &TransactionManager{
		tmMutex:    &sync.Mutex{},
		rollbacker: r,
	}
}

func (t *TransactionManager) RegisterTransaction(ctx context.Context, trans *Transaction) (*TransactionGuard, error) {
	t.tmMutex.Lock()
	defer t.tmMutex.Unlock()
	if t.transactionOngoing() {
		return nil, ErrTransactionOngoing
	}

	// no Unlock, the created transaction locks the Mutex, which must explicitly be unlocked via FinishTransaction
	t.transaction = trans

	return NewTransactionGuard(func() {
		log.Infof("Transaction: %s - canceling due to error", trans.transactionId)
		t.CleanupTransaction(trans.transactionId)
	}), nil
}

// transactionIsOngoing requires caller to acquire lock before calling
func (t *TransactionManager) transactionOngoing() bool {
	return t.transaction != nil
}

// cleanupTransaction the expectation is, that the caller is holding the lock for the TransactionManager
func (t *TransactionManager) CleanupTransaction(id string) error {
	// Perform checks by calling GetTransaction
	_, err := t.GetTransaction(id)
	if err != nil {
		return err
	}
	t.transaction = nil
	return nil
}

func (t *TransactionManager) Confirm(id string) error {
	t.tmMutex.Lock()
	defer t.tmMutex.Unlock()
	if t.transaction == nil {
		return fmt.Errorf("no ongoing transaction")
	}
	err := t.transaction.Confirm()
	if err != nil {
		return err
	}
	return t.CleanupTransaction(id)
}

func (t *TransactionManager) Cancel(ctx context.Context, id string) error {
	t.tmMutex.Lock()
	defer t.tmMutex.Unlock()
	if t.transaction == nil {
		return fmt.Errorf("no ongoing transaction")
	}
	rollbacktransAction := t.transaction.GetRollbackTransaction()

	_, err := t.rollbacker.TransactionRollback(ctx, rollbacktransAction, false)
	if err != nil {
		return err
	}
	return t.CleanupTransaction(id)
}

func (t *TransactionManager) GetTransaction(id string) (*Transaction, error) {
	if t.transaction == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	if t.transaction.transactionId != id {
		return nil, fmt.Errorf("transaction id %s is invalid", id)
	}
	return t.transaction, nil
}

func (t *TransactionManager) Rollback(ctx context.Context, trans *Transaction) error {
	t.tmMutex.Lock()
	defer t.tmMutex.Unlock()
	_, err := t.rollbacker.TransactionRollback(ctx, trans, false)

	t.transaction = nil

	return err
}
