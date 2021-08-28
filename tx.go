package pgdriver

import (
	"context"

	pgx "github.com/jackc/pgx/v4"
)

type wrapTx struct {
	ctx context.Context
	tx  pgx.Tx
}

func (wtx wrapTx) Commit() error { return wtx.tx.Commit(wtx.ctx) }

func (wtx wrapTx) Rollback() error { return wtx.tx.Rollback(wtx.ctx) }

type fakeTx struct{}

func (fakeTx) Commit() error { return nil }

func (fakeTx) Rollback() error { return nil }
