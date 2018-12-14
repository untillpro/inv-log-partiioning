package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
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

type procExec func(key primaryKey, chSum chan int, addTime func())

type procSum func(sum int)

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
	var keySpace string
	var batchSize int
	var useBatch bool
	flag.StringVar(&op, "op", "", "operation: 'insert' or 'select'")
	flag.IntVar(&wid, "wid", 1, "workspaceId")
	flag.IntVar(&threadsAmount, "threads", 1, "threads amount used for reading")
	flag.IntVar(&perDayAmount, "perDay", DefaultPerDayAmount, "per day amount to insert or read")
	flag.IntVar(&daysAmount, "days", DefaultDaysAmount, "days to insert or read")
	flag.BoolVar(&doWarmup, "no-warmup", false, "skips warmup")
	flag.Var(&clp, "cl", "consistency level (Any, 1-One, 2-Two, 3-Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne), default One")
	flag.StringVar(&host, "host", "127.0.0.1", "host ip to connect to")
	flag.IntVar(&repFactor, "rp", 3, "replication factor on keyspace create")
	flag.StringVar(&keySpace, "keySpace", "example", "keySpace name used to read and insert")
	flag.IntVar(&batchSize, "batchSize", 100, "insert batch size")
	flag.BoolVar(&useBatch, "noBatch", false, "do no use batches at all on insert. Overrides batchSize.")
	flag.Parse()

	if !clSpecified() {
		clp.cl = DefaultConsistencyLevel
	}

	doWarmup = !doWarmup
	useBatch = !useBatch

	startDT := time.Now()
	switch op {
	case "insert":
		testInsert(wid, daysAmount, perDayAmount, clp.cl, host, repFactor, threadsAmount, keySpace, batchSize, useBatch)
	case "select":
		sel(threadsAmount, doWarmup, daysAmount, wid, clp.cl, host, keySpace)
	default:
		flag.Usage()
	}
	if op != "" {
		fmt.Println("total time: ", time.Since(startDT))
	}
}

func sel(threadsAmount int, doWarmup bool, daysAmount int, wid int, cl gocql.Consistency, host string, keySpace string) {
	session := getSession(cl, keySpace, host)
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
	fmt.Println("read time: ", time.Since(wallTimeStart))

	fmt.Println(t.Calc().String())
}

func prepareGoRoutinesToBenchmark(chSum chan int, threadsAmount int, session *gocql.Session, t *tachymeter.Tachymeter, f procExec) (ch chan primaryKey, wg *sync.WaitGroup) {
	ch = make(chan primaryKey, threadsAmount)
	wg = &sync.WaitGroup{}
	for i := 1; i <= threadsAmount; i++ {
		go func(ch chan primaryKey) {
			wg.Add(1)
			for key := range ch {
				start := time.Now()
				f(key, chSum, func() {
					if t != nil {
						t.AddTime(time.Since(start))
					}
				})
			}
			wg.Done()
		}(ch)
	}
	return ch, wg
}

func createSumProcAndGetChan(f procSum, threadsAmount int) (chSum chan int) {
	chSum = make(chan int, threadsAmount)
	go func() {
		sum := 0
		startDT := time.Now()
		for add := range chSum {
			if add == 0 {
				continue
			}
			sum += add
			if time.Since(startDT).Seconds() > 1 {
				f(sum)
				startDT = time.Now()
			}
		}
		f(sum)
	}()
	return
}

func testSelect(session *gocql.Session, threadsAmount int, t *tachymeter.Tachymeter, daysAmount int, wid int) {
	funcRead := func(sum int) {
		fmt.Println("read: ", sum)
	}
	chSum := createSumProcAndGetChan(funcRead, threadsAmount)
	funcExec := func(key primaryKey, chSum chan int, addTime func()) {
		q := session.Query(`
			select WorkspaceId, Year, Month, Day, Hour, Minute, Second, Millisecond, DeviceId, UtcOffsetMinutes, Completed, Requests, Results from log 
			where workspaceid = ? and year = ? and month = ? and day = ?`, key.workspaceid, key.year, key.month, key.day)
		iter := q.Iter()
		rec := record{primaryKey: &primaryKey{}}
		j := 0
		testSum := 0
		for iter.Scan(&rec.workspaceid, &rec.year, &rec.month, &rec.day, &rec.hour, &rec.minute, &rec.second, &rec.millisecond, &rec.deviceId, &rec.utcOffsetMinutes,
			&rec.completed, &rec.requests, &rec.results) {
			j++
			if j%100 == 0 {
				chSum <- 100
			}
			testSum += int(rec.requests[1023])
		}

		q = session.Query("select count(*) from log where workspaceid = ? and year = ? and month = ? and day = ?", key.workspaceid, key.year, key.month, key.day)
		var count int
		q.Scan(&count)
		if count != 1000 {
			log.Panicln("wrong count:", count)
			q = session.Query("select count(*) from log where workspaceid = ? and year = ? and month = ? and day = ?", key.workspaceid, key.year, key.month, key.day)
			q.Scan(&count)
			if count != 1000 {
				log.Panicln("wrong count:", count)
			}
		}

		if j != 1000 {
			log.Panicln("wrong j:", j)
		}

		if testSum != 5000 {
			log.Panicln("wrong sum", testSum)
		}

		if err := iter.Close(); err != nil {
			panic(err)
		}
		addTime()
	}

	walkThroughDaysAndBenchmark(daysAmount, wid, chSum, threadsAmount, session, funcExec, t)
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

func testInsert(workspaceId int, daysAmount int, perDayAmount int, cl gocql.Consistency, host string, repFactor int, threadsAmount int, keySpace string, batchSize int, useBatch bool) {
	startDT := time.Now()
	prepareTables(cl, host, repFactor, keySpace)
	fmt.Println("prepare tables:", time.Since(startDT))

	session := getSession(cl, keySpace, host)
	defer session.Close()

	funcSum := func(sum int) {
		fmt.Println("insert: ", sum)
	}
	chSum := createSumProcAndGetChan(funcSum, threadsAmount)
	funcInsert := func(key primaryKey, chSum chan int, addTime func()) {
		var b *gocql.Batch
		if useBatch {
			b = session.NewBatch(gocql.UnloggedBatch)
		}
		for j := 1; j <= perDayAmount; j++ {
			stmt, values := getInsertQuery(key, j)
			if useBatch {
				b.Query(stmt, values...)
				if b.GetConsistency() != cl {
					log.Println("wrong consistency ", b.GetConsistency())
				}
				if j%batchSize == 0 {
					if err := session.ExecuteBatch(b); err != nil {
						panic(err)
					}
					b = session.NewBatch(gocql.UnloggedBatch)
					addTime()
					chSum <- batchSize
				}
			} else {
				q := session.Query(stmt, values...)
				if err := q.Exec(); err != nil {
					log.Panicln(err)
				}
				addTime()
				if j%batchSize == 0 {
					chSum <- batchSize
				}
			}
		}
		if useBatch {
			if err := session.ExecuteBatch(b); err != nil {
				panic(err)
			}
			chSum <- b.Size()
		}
	}

	startDT = time.Now()
	walkThroughDaysAndBenchmark(daysAmount, workspaceId, chSum, threadsAmount, session, funcInsert, tachymeter.New(&tachymeter.Config{Size: 50}))
	fmt.Println("insert time:", time.Since(startDT))
}

func walkThroughDaysAndBenchmark(daysAmount int, workspaceId int, chSum chan int, threadsAmount int, session *gocql.Session, f procExec, t *tachymeter.Tachymeter) {
	wallTimeStart := time.Now()
	ch, wg := prepareGoRoutinesToBenchmark(chSum, threadsAmount, session, t, f)

	dt := testDT
	for i := 0; i < daysAmount; i++ {
		key := primaryKey{workspaceId, dt.Year(), int(dt.Month()), dt.Day()}
		ch <- key
		dt = dt.AddDate(0, 0, 1)
	}
	close(ch)
	wg.Wait()
	close(chSum)
	if t != nil {
		t.SetWallTime(time.Since(wallTimeStart))
		fmt.Println(t.Calc().String())
	}
}

func getInsertQuery(key primaryKey, counterIncrement int) (stmt string, values []interface{}) {
	req := make([]byte, 1024)
	rand.Read(req)
	req[1023] = 5

	currentDate := time.Date(key.year, time.Month(key.month), key.day, 0, 0, 0, 0, time.UTC)
	counter := int(currentDate.Sub(testDT).Hours())/24*1000 + counterIncrement
	stmt = `INSERT INTO log (WorkspaceId, Year, Month, Day, Hour, Minute, Second, Millisecond, DeviceId, UtcOffsetMinutes, Completed, Requests, Results) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	values = append(values, key.workspaceid, key.year, key.month, key.day, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(255)-127, rand.Intn(65535)-32767,
		counter, rand.Intn(65535)-32767, true, req, []byte{})
	return
}

func prepareTables(cl gocql.Consistency, host string, repFactor int, keySpace string) {
	session := getSession(cl, "", host)
	defer session.Close()
	fmt.Print("dropping keyspace...")
	if err := session.Query(fmt.Sprintf("drop keyspace if exists %s", keySpace)).Exec(); err != nil {
		panic(err)
	}
	fmt.Println("done")

	fmt.Print("creating keyspace...")
	if err := session.Query(fmt.Sprintf("create keyspace %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }", keySpace, repFactor)).Exec(); err != nil {
		panic(err)
	}
	fmt.Println("done")

	fmt.Print("creating table...")
	if err := session.Query(fmt.Sprintf(`
		create table %s.log (
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
		)`, keySpace)).Exec(); err != nil {
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
