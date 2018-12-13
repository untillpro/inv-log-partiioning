package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/jamiealquiza/tachymeter"
)

const (
	cCBootstrapTimeout          = 3000 * time.Second
	DefaultDaysAmount       int = 365
	DefaultPerDayAmount     int = 1000
	DefaultWorkspaceId      int = 1
	DefaultConsistencyLevel     = gocql.One
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
	var repFactor int
	flag.StringVar(&op, "op", "", "operation: 'insert' or 'select'")
	flag.IntVar(&wid, "wid", 1, "workspaceId")
	flag.IntVar(&threadsAmount, "threads", 1, "threads amount used for reading")
	flag.IntVar(&perDayAmount, "perDay", DefaultPerDayAmount, "per day amount to insert or read")
	flag.IntVar(&daysAmount, "days", DefaultDaysAmount, "days to insert or read")
	flag.BoolVar(&doWarmup, "no-warmup", false, "skips warmup")
	flag.Var(&clp, "cl", "consistency level (Any, 1-One, 2-Two, 3-Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne), default One")
	flag.StringVar(&host, "host", "127.0.0.1", "host ip to connect to")
	flag.IntVar(&repFactor, "rp", 3, "replication factor on keyspace create")
	flag.Parse()

	if !clSpecified() {
		clp.cl = DefaultConsistencyLevel
	}

	doWarmup = !doWarmup

	startDT := time.Now()
	switch op {
	case "insert":
		newInsert(wid, daysAmount, perDayAmount, clp.cl, host, repFactor, threadsAmount)
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

	chSum := make(chan int)

	go func() {
		sum := 0
		startDT := time.Now()
		for read := range chSum {
			if read == 0 {
				continue
			}
			sum += read
			if time.Since(startDT).Seconds() > 1 {
				fmt.Println("read: ", sum)
				startDT = time.Now()
			}
		}
		fmt.Println("read: ", sum)
	}()

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
				j := 1
				testSum := 0
				for iter.Scan(&rec.workspaceid, &rec.year, &rec.month, &rec.day, &rec.hour, &rec.minute, &rec.second, &rec.millisecond, &rec.deviceId, &rec.utcOffsetMinutes,
					&rec.completed, &rec.requests, &rec.results) {
					j++
					if j%100 == 0 {
						chSum <- 100
					}
					testSum += int(rec.requests[1023])
				}
				if testSum != 5000
				   panic("test failed, sum", testSum)
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
		dt = dt.AddDate(0, 0, 1)
	}
	for _, ch := range chs {
		close(ch)
	}
	wg.Wait()
	close(chSum)
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

func newInsert(workspaceId int, daysAmount int, perDayAmount int, cl gocql.Consistency, host string, repFactor int, threadsAmount int) {
	startDT := time.Now()
	prepareTables(cl, host, repFactor)
	fmt.Println("prepare tables:", time.Since(startDT))

	session := getSession(cl, "example", host)
	defer session.Close()

	chs := make(map[int]chan primaryKey)
	var wg sync.WaitGroup

	chSum := make(chan int)
	t := tachymeter.New(&tachymeter.Config{Size: 50})
	wallTimeStart := time.Now()

	go func() {
		sum := 0
		startDT := time.Now()
		for inserted := range chSum {
			if inserted == 0 {
				continue
			}
			sum += inserted
			if time.Since(startDT).Seconds() > 1 {
				fmt.Println("read: ", sum)
				startDT = time.Now()
			}
		}
	}()

	for i := 1; i <= threadsAmount; i++ {
		ch := make(chan primaryKey)
		chs[i] = ch
		go func(ch chan primaryKey) {
			wg.Add(1)
			for key := range ch {
				b := gocql.NewBatch(gocql.UnloggedBatch)
				for j := 1; j <= perDayAmount; j++ {
					req := make([]byte, 1024)
					rand.Read(req)
					req[1023] = 5
					b.Query(`
					INSERT INTO log (WorkspaceId, Year, Month, Day, Hour, Minute, Second, Millisecond, DeviceId, UtcOffsetMinutes, Completed, Requests, Results) 
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
						key.workspaceid, key.year, key.month, key.day, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(65535)-32767,
						rand.Intn(65535), rand.Intn(65535)-32767, true, req, []byte{})
					//rec.workspaceid, rec.year, rec.month, rec.day, rec.hour, rec.minute, rec.second, rec.millisecond, rec.deviceId, rec.utcOffsetMinutes, rec.completed, rec.requests, rec.results)
					start := time.Now()
					if j%100 == 0 {
						if err := session.ExecuteBatch(b); err != nil {
							panic(err)
						}
						b = gocql.NewBatch(gocql.UnloggedBatch)
						t.AddTime(time.Since(start))
						chSum <- 100
					}
					b.Size()
				}
				if err := session.ExecuteBatch(b); err != nil {
					panic(err)
				}
				chSum <- b.Size()
			}
			wg.Done()
		}(ch)
	}

	startDT = time.Now()
	dt := testDT
	currentCh := 1
	for i := 0; i < daysAmount; i++ {
		key := primaryKey{workspaceId, dt.Year(), int(dt.Month()), dt.Day()}
		chs[currentCh] <- key
		currentCh++
		if currentCh >= threadsAmount {
			currentCh = 1
		}
		dt = dt.AddDate(0, 0, 1)
	}
	for _, ch := range chs {
		close(ch)
	}
	wg.Wait()
	t.SetWallTime(time.Since(wallTimeStart))
	close(chSum)
	fmt.Println(t.Calc().String())
	fmt.Println("insert time:", time.Since(startDT))
}

func prepareTables(cl gocql.Consistency, host string, repFactor int) {
	session := getSession(cl, "", host)
	defer session.Close()
	fmt.Print("dropping keyspace...")
	if err := session.Query("drop keyspace if exists example").Exec(); err != nil {
		panic(err)
	}
	fmt.Println("done")

	fmt.Print("creating keyspace...")
	if err := session.Query(fmt.Sprintf("create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }", repFactor)).Exec(); err != nil {
		panic(err)
	}
	fmt.Println("done")

	fmt.Print("creating table...")
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
	fmt.Println("done")
}

func clSpecified() bool {
	for _, str := range os.Args {
		if strings.Contains(str, "-cl") {
			return true
		}
	}
	return false
}
