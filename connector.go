package pgdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type ctxKey int

const ctxKeyFakeTx ctxKey = 0

// Only intrinsic types should be binary format with database/sql.
var databaseSQLResultFormats pgx.QueryResultFormatsByOID

var _ driver.Connector = (*Connector)(nil)

type Connector struct {
	driver driver.Driver
	pool   *pgxpool.Pool
}

func NewConnector(driver driver.Driver, dsn string) (*Connector, error) {
	pool, err := pgxpool.Connect(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to pgxpool failed, %w", err)
	}

	return &Connector{
		driver: driver,
		pool:   pool,
	}, nil
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return NewConn(conn, c.driver), nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}

var _ driver.Conn = (*Conn)(nil)

type Conn struct {
	poolConn         *pgxpool.Conn
	driver           driver.Driver
	psCount          int64                                  // Counter used for creating unique prepared statement names
	resetSessionFunc func(context.Context, *pgx.Conn) error // Function is called before a connection is reused
}

func NewConn(poolConn *pgxpool.Conn, driver driver.Driver) driver.Conn {
	return &Conn{
		poolConn:         poolConn,
		driver:           driver,
		resetSessionFunc: func(_ context.Context, _ *pgx.Conn) error { return nil }, // 默认不做任何事情
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.rawConn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	name := fmt.Sprintf("pgx_%d", c.psCount)
	c.psCount++

	sd, err := c.rawConn().Prepare(ctx, name, query)
	if err != nil {
		return nil, err
	}

	return NewStmt(c, sd), nil
}

func (c *Conn) Close() error {
	// 连接关闭时，归还到连接池中
	c.poolConn.Release()
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.rawConn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	if pconn, ok := ctx.Value(ctxKeyFakeTx).(**pgx.Conn); ok {
		*pconn = c.rawConn()
		return fakeTx{}, nil
	}

	var pgxOpts pgx.TxOptions
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		pgxOpts.IsoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		pgxOpts.IsoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		pgxOpts.IsoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		pgxOpts.IsoLevel = pgx.Serializable
	default:
		return nil, fmt.Errorf("unsupported isolation: %v", opts.Isolation)
	}

	if opts.ReadOnly {
		pgxOpts.AccessMode = pgx.ReadOnly
	}

	tx, err := c.rawConn().BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return wrapTx{ctx: ctx, tx: tx}, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if c.rawConn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	commandTag, err := c.rawConn().Exec(ctx, query, args...)
	// if we got a network error before we had a chance to send the query, retry
	if err != nil {
		if pgconn.SafeToRetry(err) {
			return nil, driver.ErrBadConn
		}
	}
	return driver.RowsAffected(commandTag.RowsAffected()), err
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if c.rawConn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := []interface{}{databaseSQLResultFormats}
	args = append(args, namedValueToInterface(argsV)...)

	rows, err := c.rawConn().Query(ctx, query, args...)
	if err != nil {
		if pgconn.SafeToRetry(err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	// Preload first row because otherwise we won't know what columns are available when database/sql asks.
	more := rows.Next()
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return NewRows(c, rows, true, more), nil
}

func (c *Conn) Ping(ctx context.Context) error {
	if c.rawConn().IsClosed() {
		return driver.ErrBadConn
	}

	err := c.rawConn().Ping(ctx)
	if err != nil {
		// A Ping failure implies some sort of fatal state. The connection is almost certainly already closed by the
		// failure, but manually close it just to be sure.
		c.Close()
		return driver.ErrBadConn
	}

	return nil
}

func (c *Conn) CheckNamedValue(*driver.NamedValue) error {
	// Underlying pgx supports sql.Scanner and driver.Valuer interfaces natively. So everything can be passed through directly.
	return nil
}

func (c *Conn) ResetSession(ctx context.Context) error {
	if c.rawConn().IsClosed() {
		return driver.ErrBadConn
	}
	if c.resetSessionFunc != nil {
		return c.resetSessionFunc(ctx, c.rawConn())
	}
	return nil
}

func (c *Conn) rawConn() *pgx.Conn {
	return c.rawConn()
}
