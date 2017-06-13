package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/DataDog/go-sqlite3"
)

func appendTo(sts []*sql.Stmt) {
	stappend := func(st *sql.Stmt) {
		num := rand.Intn(25)
		str := strconv.Itoa(rand.Intn(10))
		_, err := st.Exec(num, str)
		if err != nil {
			panic(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(sts))
	for _, st := range sts {
		go func(st *sql.Stmt) {
			stappend(st)
			wg.Done()
		}(st)
	}
	wg.Wait()
}

func BenchmarkMultiWrite(b *testing.B) {
	tmp := os.Getenv("TMP")
	if len(tmp) == 0 {
		tmp = "/tmp"
	}
	paths := []string{
		path.Join(tmp, "__sqb1.db"),
		path.Join(tmp, "__sqb2.db"),
		path.Join(tmp, "__sqb3.db"),
	}

	defer func() {
		for _, path := range paths {
			os.Remove(path)
		}
	}()
	var dsns []string
	for _, path := range paths {
		dsns = append(dsns, fmt.Sprintf("file:%s?_busy_timeout=5", path))
	}

	var dbs []*sql.DB
	for _, dsn := range dsns {
		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			b.Fatalf("error %s", err)
		}
		_, err = db.Exec("CREATE TABLE test (a int, b text);")
		if err != nil {
			b.Fatalf("ddl error %s", err)
		}
		dbs = append(dbs, db)
	}

	var stmts []*sql.Stmt
	for _, db := range dbs {
		stmt, err := db.Prepare("INSERT OR REPLACE INTO test (a, b) VALUES (?, ?);")
		if err != nil {
			b.Fatalf("prepare %s", err)
		}
		stmts = append(stmts, stmt)
	}

	b.ResetTimer()

	var tds []int
	t0 := time.Now()
	for i := 0; i < b.N; i++ {
		appendTo(stmts)
		td := time.Since(t0)
		tds = append(tds, int(td))
		t0 = time.Now()
	}

	b.StopTimer()

	sort.Sort(sort.Reverse(sort.IntSlice(tds)))

	if len(tds) > 3 && tds[0] > int(time.Millisecond*100) {
		var sum int
		for _, td := range tds {
			sum += td
		}
		avg := time.Duration(sum / len(tds))
		// not the real median but close enough
		median := time.Duration(tds[len(tds)/2])
		b.Logf("slow bench, top 3 (median: %s, avg: %s):", median, avg)
		for _, td := range tds[:3] {
			b.Logf(" %s", time.Duration(td))
		}
	}

	total := 0
	for _, db := range dbs {
		rows, err := db.Query("SELECT COUNT(*) FROM test;")
		if err != nil {
			b.Fatalf("select err %s", err)
		}
		if !rows.Next() {
			b.Fatal("no next")
		}
		var num int
		err = rows.Scan(&num)
		if err != nil {
			b.Fatalf("scan err %s", err)
		}
		rows.Close()
		total += num
	}

	for _, db := range dbs {
		db.Close()
	}

	b.Log("total rows:", total)
}
