package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

const (
	cCBootstrapTimeout     = 3000 * time.Second
	daysAmount         int = 365
	perDayAmount       int = 3000
)

type params struct {
	year  int
	month int
	day   int
}

var testDT = time.Date(2018, 06, 06, 0, 0, 0, 0, time.UTC)

func main() {
	if len(os.Args) == 0 {
		usage()
	}
	startDT := time.Now()
	switch os.Args[1] {
	case "insert":
		insert()
	case "select":
		threadsAmount, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		sel(threadsAmount)
	default:
		usage()
	}
	fmt.Printf("total time: %vms\n", time.Now().Sub(startDT).Seconds()*1000.)
}

func usage() {
	panic("cmd line: [insert|select threadsAmount]")
}

func sel(threadsAmount int) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = cCBootstrapTimeout

	startDT := time.Now()
	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	ch := make(chan params)
	var wg sync.WaitGroup

	for i := 1; i <= threadsAmount; i++ {
		go func() {
			wg.Add(1)
			for params := range ch {
				q := session.Query("SELECT * from log where workspaceid = ? and year = ? and month = ? and day = ?", params.year, params.month, params.day)
				iter := q.Iter()
				for iter.Scan() {

				}
				if err := iter.Close(); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	dt := testDT
	for i := 0; i < daysAmount; i++ {
		param := params{dt.Year(), int(dt.Month()), dt.Day()}
		ch <- param
		dt.AddDate(0, 0, 1)
	}

	close(ch)
	wg.Wait()

	fmt.Printf("Read time %vms\n", time.Since(startDT).Seconds()*1000.)
}

func insert() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = cCBootstrapTimeout

	startDT := time.Now()
	prepareTables(cluster)
	fmt.Printf("prepare tables: %vms\n", time.Now().Sub(startDT).Seconds()*1000.)

	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	startDT = time.Now()
	tm := testDT
	for i := int32(0); i < daysAmount; i++ {
		b := gocql.NewBatch(gocql.LoggedBatch)
		for j := 0; j < perDayAmount; j++ {

			req := make([]byte, 1024)
			rand.Read(req)
			b.Query(`
			INSERT INTO log ( 
				WorkspaceId,
				Year,
				Month,
				Day,
				Hour,
				Minute,
				Second,
				Millisecond,
				DeviceId,
				UtcOffsetMinutes,
				Completed,
				Requests,
				Results
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				1, tm.Year(), tm.Month(), tm.Day(), rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(65535)-32767,
				rand.Intn(65535), rand.Intn(65535)-32767, true, req, []byte{})
			if j%100 == 0 {
				if err := session.ExecuteBatch(b); err != nil {
					panic(err)
				}
			}
		}

		tm = tm.AddDate(0, 0, 1)
	}
	fmt.Printf("fill DB: %vms\n", time.Now().Sub(startDT).Seconds()*1000.)
}

func prepareTables(cluster *gocql.ClusterConfig) {
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()
	if err := session.Query("drop keyspace if exists example").Exec(); err != nil {
		panic(err)
	}

	if err := session.Query("create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").Exec(); err != nil {
		panic(err)
	}

	if err := session.Query(`
		create table example.log (
			WorkspaceId bigint,
			        Year smallint,
			        Month tinyint,
			        Day tinyint,
			        Hour tinyint,
			        Minute tinyint,
			        Second tinyint,
			        Millisecond smallint,
			        DeviceId int,
			        UtcOffsetMinutes smallint,
			        Completed boolean,
			        Requests blob,
			        Results blob,
			        PRIMARY KEY ((WorkspaceId, Year, Month, Day), Hour, Minute, Second, Millisecond, DeviceId)
		)`).Exec(); err != nil {
		panic(err)
	}
}
