// This example uses ticket to amortize SQL transaction overhead
package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/echlebek/ticket"
	"golang.org/x/sync/errgroup"
	_ "modernc.org/sqlite"
)

type QueryValues struct {
	Name string
	Bork int
}

type StmtJobHandler[Job QueryValues] struct {
	Stmt *sql.Stmt
	DB *sql.DB
}

func (j *StmtJobHandler[QueryValues]) Handle(ctx context.Context, values []QueryValues) (err error) {
	tx, txerr := j.DB.Begin()
	if txerr != nil {
		return err
	}
	defer func () {
		err = tx.Commit()
	}()
	stmt := tx.Stmt(j.Stmt)
	for _, value := range values {
		if _, err := stmt.Exec(value.Name, value.Bork); err != nil {
			return err
		}
	}
	return nil
}

var borksOutOf10 = map[string]int {
	"susie": 13,
	"sheena": 13,
	"cody": 10,
	"clause": 10,
	"franklin": 11,
	"oreo": 14,
	"kiki": 1000000,
}

func egFunc(f func (ctx context.Context) error) func() error {
	return func() error {
		return f(context.Background())
	}
}

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`CREATE TABLE dogs ( name TEXT, bork INTEGER );`)
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(`INSERT INTO dogs ( name, bork ) VALUES ( ?, ? )`)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	handler := StmtJobHandler[QueryValues]{
		DB: db,
		Stmt: stmt,
	}
	server := ticket.NewBatchServer[QueryValues](ctx, 100, time.Millisecond, &handler)
	var eg errgroup.Group
	for i := 0; i < 1000; i++ {
		for name, borks := range borksOutOf10 {
			eg.Go(egFunc(server.Accept(QueryValues{Name: name, Bork: borks}).Wait))
		}
	}
	if err := eg.Wait(); err != nil {
		panic(err)
	}
}
