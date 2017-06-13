package main

import (
	"database/sql"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/DataDog/go-sqlite3"
)

func appendTo(dbs []*sql.DB) {
	append := func(db *sql.DB) {
		num := rand.Intn(25)
		str := strconv.Itoa(rand.Intn(10))
		_, err := db.Exec("INSERT OR REPLACE INTO test (a, b) VALUES (?, ?);", num, str)
		if err != nil {
			panic(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(dbs))
	for _, db := range dbs {
		go func(db *sql.DB) {
			append(db)
			wg.Done()
		}(db)
	}
	wg.Wait()
}

func BenchmarkMultiWrite(b *testing.B) {
	paths := []string{"/tmp/__sqb1.db", "/tmp/__sqb2.db", "/tmp/__sqb3.db"}
	defer func() {
		for _, path := range paths {
			os.Remove(path)
		}
	}()

	var dbs []*sql.DB
	for _, path := range paths {
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			b.Fatalf("error %s", err)
		}
		_, err = db.Exec("CREATE TABLE test (a int, b text);")
		if err != nil {
			b.Fatalf("ddl error %s", err)
		}
		dbs = append(dbs, db)
	}

	b.ResetTimer()

	var tds []int
	t0 := time.Now()
	for i := 0; i < b.N; i++ {
		appendTo(dbs)
		td := time.Since(t0)
		tds = append(tds, int(td))
		t0 = time.Now()
	}

	b.StopTimer()

	sort.Sort(sort.Reverse(sort.IntSlice(tds)))

	if len(tds) > 3 && tds[0] > int(time.Millisecond*100) {
		b.Log("slow bench, top 3:")
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
