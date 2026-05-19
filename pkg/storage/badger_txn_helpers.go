package storage

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerEngine) ensureOpen() error {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return ErrStorageClosed
	}
	return nil
}

func (b *BadgerEngine) withView(fn func(txn *badger.Txn) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return recoverBadgerClosedPanic(func() error {
		return b.db.View(fn)
	})
}

func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	var nodeMax, edgeMax uint64
	var propKeyDrain propKeyTxnDrain
	err := recoverBadgerClosedPanic(func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			if err := fn(txn); err != nil {
				if b.idDict != nil {
					b.idDict.discardTxnCounters(txn)
				}
				if b.propKeyDict != nil {
					b.propKeyDict.discardTxnCounters(txn)
				}
				return err
			}
			// Drain the staged counter high-water marks and any
			// pending property-key forward/reverse entries so they
			// can be persisted out-of-band (see flushTxnCounters
			// doc — these keys cannot ride the user txn or
			// concurrent writers race on them).
			if b.idDict != nil {
				nodeMax, edgeMax = b.idDict.flushTxnCounters(txn)
			}
			if b.propKeyDict != nil {
				propKeyDrain = b.propKeyDict.flushTxnCounters(txn)
			}
			return nil
		})
	})
	if err == nil {
		if b.idDict != nil {
			b.idDict.persistCounters(b.db, nodeMax, edgeMax)
		}
		if b.propKeyDict != nil {
			b.propKeyDict.persistTxnCounters(b.db, propKeyDrain)
		}
	}
	return err
}

func recoverBadgerClosedPanic(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if isBadgerClosedPanic(recovered) {
				err = ErrStorageClosed
				return
			}
			panic(recovered)
		}
	}()

	return fn()
}

func isBadgerClosedPanic(recovered interface{}) bool {
	message := fmt.Sprint(recovered)
	return strings.Contains(message, "DB Closed")
}
