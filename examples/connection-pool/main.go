package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ConnectionTester struct {
	dsn           string
	targetConns   int
	duration      time.Duration
	query         string
	successCount  int32
	activeConns   int32
	sigChan       chan os.Signal
	wg            sync.WaitGroup
	startTime     time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	exitOnFailure bool
}

func NewConnectionTester(host string, port int, user, password, database string,
	connections int, duration time.Duration, query string, exitOnFailure bool) *ConnectionTester {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, database)
	ctx, cancel := context.WithCancel(context.Background())

	return &ConnectionTester{
		dsn:           dsn,
		targetConns:   connections,
		duration:      duration,
		query:         query,
		sigChan:       make(chan os.Signal, 1),
		ctx:           ctx,
		cancel:        cancel,
		exitOnFailure: exitOnFailure,
	}
}

func (ct *ConnectionTester) GetActiveConnections() int32 {
	return atomic.LoadInt32(&ct.activeConns)
}

func (ct *ConnectionTester) GetSuccessCount() int32 {
	return atomic.LoadInt32(&ct.successCount)
}

func (ct *ConnectionTester) establishConnection(connID int) {
	defer ct.wg.Done()
	defer atomic.AddInt32(&ct.activeConns, -1)

	atomic.AddInt32(&ct.activeConns, 1)

	db, err := sql.Open("mysql", ct.dsn)
	if err != nil {
		log.Printf("Connection %d failed to open: %v", connID, err)
		if ct.exitOnFailure {
			ct.cancel()
			os.Exit(1)
		}
		return
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Printf("Connection %d ping failed: %v", connID, err)
		if ct.exitOnFailure {
			ct.cancel()
			os.Exit(1)
		}
		return
	}

	rows, err := db.Query(ct.query)
	if err != nil {
		log.Printf("Connection %d query failed: %v", connID, err)
		if ct.exitOnFailure {
			ct.cancel()
			os.Exit(1)
		}
		return
	}
	rows.Close()

	atomic.AddInt32(&ct.successCount, 1)
	current := atomic.LoadInt32(&ct.successCount)
	log.Printf("Connection %d established successfully (Total: %d)", connID, current)

	timer := time.NewTimer(ct.duration)
	defer timer.Stop()

	select {
	case <-ct.ctx.Done():
		log.Printf("Connection %d received shutdown signal", connID)
		return
	case <-timer.C:
		log.Printf("Connection %d timeout reached", connID)
		return
	}
}

func (ct *ConnectionTester) Run() {
	signal.Notify(ct.sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-ct.sigChan
		log.Println("Received shutdown signal. Closing all connections...")
		ct.cancel()
		signal.Stop(ct.sigChan)
	}()

	ct.startTime = time.Now()
	log.Printf("Starting test with %d connections", ct.targetConns)

	for i := 0; i < ct.targetConns; i++ {
		select {
		case <-ct.ctx.Done():
			return
		default:
			ct.wg.Add(1)
			go ct.establishConnection(i)

			if i > 0 && i%10 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	go ct.printStatus()

	done := make(chan struct{})
	go func() {
		ct.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(ct.startTime)
		log.Printf("Test completed in %v", elapsed)
		successCount := ct.GetSuccessCount()
		log.Printf("Successfully established %d out of %d connections",
			successCount, ct.targetConns)
		if ct.exitOnFailure && successCount < int32(ct.targetConns) {
			os.Exit(1)
		}
	case <-ct.ctx.Done():
		log.Println("Waiting for all connections to close...")
		ct.wg.Wait()
		log.Println("All connections closed")
		if ct.exitOnFailure {
			os.Exit(1)
		}
	}
}

func (ct *ConnectionTester) printStatus() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Printf("Status - Active connections: %d, Successful connections: %d",
				ct.GetActiveConnections(), ct.GetSuccessCount())
		case <-ct.ctx.Done():
			return
		}
	}
}

func main() {
	mysqlHost := flag.String("mysql-host", "localhost", "MySQL host")
	mysqlPort := flag.Int("mysql-port", 15306, "MySQL port")
	mysqlUser := flag.String("mysql-user", "root", "MySQL user")
	mysqlPassword := flag.String("mysql-password", "passwd", "MySQL password")
	mysqlDatabase := flag.String("mysql-database", "information_schema", "Database name")
	connectionCount := flag.Int("connection-count", 100, "Number of connections")
	duration := flag.Duration("duration", 5*time.Minute, "Duration to keep connections")
	query := flag.String("query", "SELECT 1", "Query to execute")
	exitOnFailure := flag.Bool("exit-on-failure", true, "Exit process on connection failure")

	flag.Parse()

	tester := NewConnectionTester(
		*mysqlHost, *mysqlPort, *mysqlUser, *mysqlPassword, *mysqlDatabase,
		*connectionCount, *duration, *query, *exitOnFailure,
	)

	tester.Run()
}
