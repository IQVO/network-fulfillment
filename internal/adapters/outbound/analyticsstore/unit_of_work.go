package analyticsstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is the subset of pgx that both *pgxpool.Pool and pgx.Tx satisfy,
// so a store issues its SQL either autonomously (pool) or inside the
// analytics UnitOfWork's transaction (tx) without knowing which.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// txKey carries the analytics transaction on a context. It is private to
// this package and distinct from the OLTP postgres package's key: the
// analytical database is a separate database with its own pool, so an OLTP
// transaction can never be joined here by accident.
type txKey struct{}

func withTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func txFrom(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// querierFrom resolves the analytics transaction bound to ctx, or falls back
// to pool when the caller is not inside a UnitOfWork.
func querierFrom(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := txFrom(ctx); ok {
		return tx
	}
	return pool
}

// UnitOfWork runs the analytics consumer's dedupe claim
// (ConsumedEventsRepo.MarkProcessed) and the projection write
// (PostgresProjection.Apply*) in ONE transaction on the analytical database
// (ADR 0023): either both commit or neither does, so a process stop between
// them can never leave a claim whose projection write is missing. It mirrors
// the OLTP postgres.UnitOfWork (ctx-carried tx; stores join it).
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constructs a UnitOfWork over the analytical pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Execute runs fn inside one transaction. A nested call joins the outer
// scope. On error (or panic) the transaction is rolled back; the rollback
// runs on a context detached from ctx's cancellation so a shutdown that
// interrupted fn still releases the transaction cleanly.
func (u *UnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if _, ok := txFrom(ctx); ok {
		return fn(ctx)
	}

	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("analyticsstore: begin unit of work: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("analyticsstore: rollback unit of work: %w", rbErr))
			}
		}
	}()

	if err = fn(withTx(ctx, tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("analyticsstore: commit unit of work: %w", err)
	}
	return nil
}
