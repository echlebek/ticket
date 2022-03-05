package main

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/echlebek/ticket"
	_ "modernc.org/sqlite"
)

func BenchmarkMutex(b *testing.B) {
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		os.RemoveAll("test.db")
	}()
	_, err = db.Exec(`CREATE TABLE dogs ( name TEXT, bork INTEGER );`)
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare(`INSERT INTO dogs ( name, bork ) VALUES ( ?, ? )`)
	if err != nil {
		b.Fatal(err)
	}
	var mu sync.Mutex
	var i int64
	b.ResetTimer()
	b.RunParallel(func (pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&i, 1)
			mu.Lock()
			if _, err := stmt.Exec("hello", i); err != nil {
				b.Fatal(err)
			}
			mu.Unlock()
		}
	})
}

func doTicketBench(size int, b *testing.B) {
	db, err := sql.Open("sqlite", "test2.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test2.db")
	_, err = db.Exec(`CREATE TABLE dogs ( name TEXT, bork INTEGER );`)
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare(`INSERT INTO dogs ( name, bork ) VALUES ( ?, ? )`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	handler := StmtJobHandler[QueryValues]{
		DB: db,
		Stmt: stmt,
	}
	server := ticket.NewBatchServer[QueryValues](ctx, size, time.Millisecond, &handler)
	var borks int64
	b.ResetTimer()
	b.RunParallel(func (pb *testing.PB) {
		for pb.Next() {
			borks := atomic.AddInt64(&borks, 1)
			_ = server.Accept(QueryValues{Name: "hello", Bork: int(borks)}).Wait(context.Background())
		}
	})
}

func BenchmarkTicket1(b *testing.B) {
	doTicketBench(1, b)
}

func BenchmarkTicket2(b *testing.B) {
	doTicketBench(2, b)
}

func BenchmarkTicket4(b *testing.B) {
	doTicketBench(4, b)
}

func BenchmarkTicket8(b *testing.B) {
	doTicketBench(8, b)
}

func BenchmarkTicket16(b *testing.B) {
	doTicketBench(16, b)
}

func BenchmarkTicket32(b *testing.B) {
	doTicketBench(32, b)
}

func BenchmarkTicket64(b *testing.B) {
	doTicketBench(64, b)
}
