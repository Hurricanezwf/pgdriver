package pgdriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/jackc/pgconn"
)

var _ driver.Stmt = (*Stmt)(nil)

type Stmt struct {
	conn     *Conn
	stmtDesc *pgconn.StatementDescription
}

func NewStmt(conn *Conn, stmtDesc *pgconn.StatementDescription) *Stmt {
	return &Stmt{
		conn:     conn,
		stmtDesc: stmtDesc,
	}
}

func (s *Stmt) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return s.conn.rawConn().Deallocate(ctx, s.stmtDesc.Name)
}

func (s *Stmt) NumInput() int {
	return len(s.stmtDesc.ParamOIDs)
}

func (s *Stmt) Exec(argsV []driver.Value) (driver.Result, error) {
	return nil, errors.New("Stmt.Exec deprecated and not implemented")
}

func (s *Stmt) ExecContext(ctx context.Context, argsV []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.stmtDesc.Name, argsV)
}

func (s *Stmt) Query(argsV []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Stmt.Query deprecated and not implemented")
}

func (s *Stmt) QueryContext(ctx context.Context, argsV []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.stmtDesc.Name, argsV)
}
