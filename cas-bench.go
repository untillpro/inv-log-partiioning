package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/jamiealquiza/tachymeter"
)

const (
	cCBootstrapTimeout         = 3000 * time.Second
	DefaultDaysAmount      int = 365
	DefaultPerDayAmount    int = 1000
	DefaultWorkspaceId     int = 1
	DefautConsistencyLevel     = gocql.One
)

type primaryKey struct {
	workspaceid int
	year        int
	month       int
	day         int
}

type record struct {
	*primaryKey
	hour             int
	minute           int
	second           int
	millisecond      int
	deviceId         int
	utcOffsetMinutes int
	completed        bool
	requests         []byte
	results          []byte
}

type clParser struct {
	cl gocql.Consistency
}

func (this *clParser) String() string {
	return fmt.Sprint(*this)
}

func (this *clParser) Set(str string) error {
	switch str {
	case "any":
		this.cl = gocql.Any
	case "1":
		this.cl = gocql.One
	case "2":
		this.cl = gocql.Two
	case "3":
		this.cl = gocql.Three
	case "quorum":
		this.cl = gocql.Quorum
	case "all":
		this.cl = gocql.All
	case "LocalQuorum":
		this.cl = gocql.LocalQuorum
	case "EachQuorum":
		this.cl = gocql.EachQuorum
	case "LocalOne":
		this.cl = gocql.LocalOne
	default:
		return errors.New("wrong consistency level")
	}
	return nil
}

var testDT = time.Date(2018, 06, 06, 0, 0, 0, 0, time.UTC)

func main() {
	var op string
	var wid int
	var threadsAmount int
	var perDayAmount int
	var doWarmup bool
	var daysAmount int
	var clp clParser
	var host string
	flag.StringVar(&op, "op", "", "operation: 'insert' or 'select'")
	flag.IntVar(&wid, "wid", 1, "workspaceId")
	flag.IntVar(&threadsAmount, "threads", 1, "threads amount used for reading")
	flag.IntVar(&perDayAmount, "perDay", DefaultPerDayAmount, "per day amount to insert or read")
	flag.IntVar(&daysAmount, "days", DefaultDaysAmount, "days to insert or read")
	flag.BoolVar(&doWarmup, "no-warmup", false, "skips warmup")
	flag.Var(&clp, "cl", "consistency level (Any, 1-One, 2-Two, 3-Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne)")
	flag.StringVar(&host, "host", "127.0.0.1", "host ip to connect to")
	flag.Parse()

	doWarmup = !doWarmup

	startDT := time.Now()
	switch op {
	case "insert":
		insert(wid, daysAmount, perDayAmount, clp.cl, host)
	case "select":
		sel(threadsAmount, doWarmup, daysAmount, wid, clp.cl, host)
	default:
		flag.Usage()
	}
	if op != "" {
		fmt.Println("total time: ", time.Since(startDT))
	}
}

func sel(threadsAmount int, doWarmup bool, daysAmount int, wid int, cl gocql.Consistency, host string) {
	session := getSession(cl, "example", host)
	defer session.Close()

	if doWarmup {
		fmt.Println("warming up...")
		testSelect(session, threadsAmount, nil, daysAmount, wid)
	}

	t := tachymeter.New(&tachymeter.Config{Size: 50})
	wallTimeStart := time.Now()
	fmt.Println("reading...")
	testSelect(session, threadsAmount, t, daysAmount, wid)
	t.SetWallTime(time.Since(wallTimeStart))

	fmt.Println(t.Calc().String())
}

func testSelect(session *gocql.Session, threadsAmount int, t *tachymeter.Tachymeter, daysAmount int, wid int) {
	chs := make(map[int]chan primaryKey)
	var wg sync.WaitGroup

	for i := 1; i <= threadsAmount; i++ {
		ch := make(chan primaryKey)
		chs[i] = ch
		go func(ch chan primaryKey) {
			wg.Add(1)
			for key := range ch {
				start := time.Now()
				q := session.Query(`
					select WorkspaceId, Year, Month, Day, Hour, Minute, Second, Millisecond, DeviceId, UtcOffsetMinutes, Completed, Requests, Results from log 
					where workspaceid = ? and year = ? and month = ? and day = ?`, key.workspaceid, key.year, key.month, key.day)
				iter := q.Iter()
				rec := record{primaryKey: &primaryKey{}}
				for iter.Scan(&rec.workspaceid, &rec.year, &rec.month, &rec.day, &rec.hour, &rec.minute, &rec.second, &rec.millisecond, &rec.deviceId, &rec.utcOffsetMinutes,
					&rec.completed, &rec.requests, &rec.results) {
				}
				if t != nil {
					t.AddTime(time.Since(start))
				}
				if err := iter.Close(); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(ch)
	}

	dt := testDT
	currentCh := 1
	for i := 0; i < daysAmount; i++ {
		key := primaryKey{wid, dt.Year(), int(dt.Month()), dt.Day()}
		chs[currentCh] <- key
		currentCh++
		if currentCh >= threadsAmount {
			currentCh = 1
		}
		dt.AddDate(0, 0, 1)
	}
	for _, ch := range chs {
		close(ch)
	}
	wg.Wait()
}

func getClusterConfig(cl gocql.Consistency, host string) *gocql.ClusterConfig {
	cc := gocql.NewCluster(host)
	cc.Consistency = cl
	cc.Timeout = cCBootstrapTimeout
	return cc
}

func getSession(cl gocql.Consistency, keySpace string, host string) *gocql.Session {
	cc := getClusterConfig(cl, host)
	if len(keySpace) > 0 {
		cc.Keyspace = keySpace
	}
	session, err := cc.CreateSession()
	if err != nil {
		panic(err)
	}
	return session
}

func insert(workspaceId int, daysAmount int, perDayAmount int, cl gocql.Consistency, host string) {
	startDT := time.Now()
	prepareTables(cl, host)
	fmt.Println("prepare tables:", time.Since(startDT))

	session := getSession(cl, "example", host)
	defer session.Close()

	startDT = time.Now()
	tm := testDT
	for i := 0; i < daysAmount; i++ {
		b := gocql.NewBatch(gocql.LoggedBatch)
		for j := 0; j < perDayAmount; j++ {
			req := make([]byte, 1024)
			rand.Read(req)
			req[1023] = 5
			b.Query(`
			INSERT INTO log (WorkspaceId, Year, Month, Day, Hour, Minute, Second, Millisecond, DeviceId, UtcOffsetMinutes, Completed, Requests, Results) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				workspaceId, tm.Year(), int(tm.Month()), tm.Day(), rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(65535)-32767,
				rand.Intn(65535), rand.Intn(65535)-32767, true, req, []byte{})
			if j%100 == 0 {
				if err := session.ExecuteBatch(b); err != nil {
					panic(err)
				}
			}
		}
		if err := session.ExecuteBatch(b); err != nil {
			panic(err)
		}
		tm = tm.AddDate(0, 0, 1)
	}
	fmt.Println("fill DB:", time.Since(startDT))
}

func prepareTables(cl gocql.Consistency, host string) {
	session := getSession(cl, "", host)
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
