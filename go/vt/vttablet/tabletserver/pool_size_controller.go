package tabletserver

import (
	"context"
	"fmt"
	"github.com/spf13/pflag"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/background"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

// QueryServerPoolAutoScaleConfig holds configuration parameters for PoolSizeController.
type QueryServerPoolAutoScaleConfig struct {
	Enable                     bool          // Enable the pool size controller
	DryRun                     bool          // Enable dry-run mode, it will override Enable if it is true
	Interval                   time.Duration // Interval at which pool sizes are adjusted
	PercentageOfMaxConnections int           // Percentage of MySQL max_connections that vttablet should use (e.g., 80 for 80%). range from 10 to 90.
	SafetyBuffer               int           // Number of connections to reserve as a safety buffer (e.g., 35)
	TxPoolPercentage           int           // Fraction of connections allocated to te (e.g., 0.3 for 30%)
	MinTxPoolSize              int           // Minimum size of te (e.g., 5)
	MinOltpReadPoolSize        int           // Minimum size of qe (e.g., 5)
}

var defaultConfig = QueryServerPoolAutoScaleConfig{
	Enable:                     false,
	DryRun:                     false,
	Interval:                   30 * time.Second,
	PercentageOfMaxConnections: 80,
	SafetyBuffer:               35,
	TxPoolPercentage:           50,
	MinTxPoolSize:              5,
	MinOltpReadPoolSize:        5,
}

var config = QueryServerPoolAutoScaleConfig{
	Enable:                     defaultConfig.Enable,
	DryRun:                     defaultConfig.DryRun,
	Interval:                   defaultConfig.Interval,
	PercentageOfMaxConnections: defaultConfig.PercentageOfMaxConnections,
	SafetyBuffer:               defaultConfig.SafetyBuffer,
	TxPoolPercentage:           defaultConfig.TxPoolPercentage,
	MinTxPoolSize:              defaultConfig.MinTxPoolSize,
	MinOltpReadPoolSize:        defaultConfig.MinOltpReadPoolSize,
}

func registerPoolSizeControllerConfigTypeFlags(fs *pflag.FlagSet) {
	pflag.BoolVar(&config.Enable, "queryserver_pool_autoscale_enable", config.Enable, "Enable the pool size autoscaler. default is false.")
	pflag.BoolVar(&config.DryRun, "queryserver_pool_autoscale_dry_run", config.DryRun, "Enable dry-run mode. When enabled, it overrides queryserver_pool_autoscale_enable. default is false.")
	pflag.DurationVar(&config.Interval, "queryserver_pool_autoscale_interval", config.Interval, "Interval at which pool sizes are adjusted. default is 30s.")
	pflag.IntVar(&config.PercentageOfMaxConnections, "queryserver_pool_autoscale_percentage_of_max_connections", config.PercentageOfMaxConnections, "Percentage of MySQL max_connections that vttablet should use. range from 10 to 90. default is 80.")
	pflag.IntVar(&config.SafetyBuffer, "queryserver_pool_autoscale_safety_buffer", config.SafetyBuffer, "Number of connections to reserve as a safety buffer. default is 35.")
	pflag.IntVar(&config.TxPoolPercentage, "queryserver_pool_autoscale_tx_pool_percentage", config.TxPoolPercentage, "Fraction of connections allocated to te. default is 50%.")
	pflag.IntVar(&config.MinTxPoolSize, "queryserver_pool_autoscale_min_tx_pool_size", config.MinTxPoolSize, "Minimum size of te. default is 5.")
	pflag.IntVar(&config.MinOltpReadPoolSize, "queryserver_pool_autoscale_min_oltp_read_pool_size", config.MinOltpReadPoolSize, "Minimum size of qe. default is 5.")
}

func ValidateQueryServerPoolAutoScaleConfig(useDefaultOnError bool) []error {
	var errorList []error
	if config.PercentageOfMaxConnections < 10 || config.PercentageOfMaxConnections > 90 {
		err := fmt.Errorf("invalid queryserver_pool_autoscale_percentage_of_max_connections: %d, must be between 10 and 90", config.PercentageOfMaxConnections)
		errorList = append(errorList, err)
		if useDefaultOnError {
			config.PercentageOfMaxConnections = defaultConfig.PercentageOfMaxConnections
			log.Warningf("Using default queryserver_pool_autoscale_percentage_of_max_connections: %d", config.PercentageOfMaxConnections)
		}
	}
	if config.SafetyBuffer < 0 {
		err := fmt.Errorf("invalid queryserver_pool_autoscale_safety_buffer: %d, must be greater than or equal to 0", config.SafetyBuffer)
		errorList = append(errorList, err)
		if useDefaultOnError {
			config.SafetyBuffer = defaultConfig.SafetyBuffer
			log.Warningf("Using default queryserver_pool_autoscale_safety_buffer: %d", config.SafetyBuffer)
		}
	}
	if config.TxPoolPercentage < 0 || config.TxPoolPercentage > 100 {
		err := fmt.Errorf("invalid queryserver_pool_autoscale_tx_pool_percentage: %d, must be between 0 and 100", config.TxPoolPercentage)
		errorList = append(errorList, err)
		if useDefaultOnError {
			config.TxPoolPercentage = defaultConfig.TxPoolPercentage
			log.Warningf("Using default queryserver_pool_autoscale_tx_pool_percentage: %d", config.TxPoolPercentage)
		}
	}
	if config.MinTxPoolSize < 0 {
		err := fmt.Errorf("invalid queryserver_pool_autoscale_min_tx_pool_size: %d, must be greater than or equal to 0", config.MinTxPoolSize)
		errorList = append(errorList, err)
		if useDefaultOnError {
			config.MinTxPoolSize = defaultConfig.MinTxPoolSize
			log.Warningf("Using default queryserver_pool_autoscale_min_tx_pool_size: %d", config.MinTxPoolSize)
		}
	}
	if config.MinOltpReadPoolSize < 0 {
		err := fmt.Errorf("invalid queryserver_pool_autoscale_min_oltp_read_pool_size: %d, must be greater than or equal to 0", config.MinOltpReadPoolSize)
		errorList = append(errorList, err)
		if useDefaultOnError {
			config.MinOltpReadPoolSize = defaultConfig.MinOltpReadPoolSize
			log.Warningf("Using default queryserver_pool_autoscale_min_oltp_read_pool_size: %d", config.MinOltpReadPoolSize)
		}
	}
	for _, err := range errorList {
		log.Errorf("%v", err)
	}
	return errorList
}

func init() {
	servenv.OnParseFor("vttablet", registerPoolSizeControllerConfigTypeFlags)
}

// PoolSizeController dynamically adjusts the sizes of te and qe based on MySQL metrics.
type PoolSizeController struct {
	isOpen              atomic.Bool
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	taskPool            *background.TaskPool
	tsv                 *TabletServer
	te                  *TxEngine
	qe                  *QueryEngine
	connectionErrorsMux sync.Mutex
	prevConnErrors      int
}

// NewPoolSizeController creates a new PoolSizeController with the provided configuration.
func NewPoolSizeController(tsv *TabletServer, taskPool *background.TaskPool, te *TxEngine, qe *QueryEngine) *PoolSizeController {
	return &PoolSizeController{
		tsv:            tsv,
		taskPool:       taskPool,
		te:             te,
		qe:             qe,
		prevConnErrors: -1, // Initialize to -1 to indicate no previous value
	}
}

// Open starts the PoolSizeController's monitoring routine.
func (psc *PoolSizeController) Open() {
	if psc.isOpen.Load() {
		return
	}
	log.Info("PoolSizeController opened")
	psc.ctx, psc.cancelFunc = context.WithCancel(context.Background())
	go psc.Start()
	psc.isOpen.Store(true)
}

// Close stops the PoolSizeController's monitoring routine.
func (psc *PoolSizeController) Close() {
	if !psc.isOpen.Load() {
		return
	}
	log.Info("PoolSizeController closed")
	psc.cancelFunc()
	psc.isOpen.Store(false)
}

// Start runs the monitoring loop that adjusts pool sizes at regular intervals.
func (psc *PoolSizeController) Start() {
	t := time.NewTicker(config.Interval)
	defer t.Stop()
	for {
		select {
		case <-psc.ctx.Done():
			return
		case <-t.C:
			psc.Reconcile()
		}
	}
}

// Reconcile adjusts the te and qe sizes based on MySQL metrics.
func (psc *PoolSizeController) Reconcile() {
	conn, err := psc.taskPool.BorrowConn(psc.ctx, nil)
	if err != nil {
		log.Errorf("Failed to acquire connection from task pool: %v", err)
		return
	}
	defer conn.Recycle()

	// Fetch MySQL metrics
	maxConnections, err := psc.getGlobalVariable(conn, "max_connections")
	if err != nil {
		log.Errorf("Failed to fetch max_connections: %v", err)
		return
	}

	//threadsConnected, err := psc.getGlobalStatus(conn, "Threads_connected")
	//if err != nil {
	//	log.Errorf("Failed to fetch Threads_connected: %v", err)
	//	return
	//}

	// Fetch Connection_errors_max_connections
	connectionErrors, err := psc.getGlobalStatus(conn, "Connection_errors_max_connections")
	if err != nil {
		log.Errorf("Failed to fetch Connection_errors_max_connections: %v", err)
		return
	}

	// Monitor Connection_errors_max_connections
	psc.handleConnectionErrors(connectionErrors)

	// Apply the algorithm to calculate pool sizes

	// Calculate the maximum connections vttablet should use
	vttabletMaxConnections := (maxConnections * config.PercentageOfMaxConnections) / 100

	// Adjust for safety buffer only
	availableConnections := maxConnections - config.SafetyBuffer
	if availableConnections < vttabletMaxConnections {
		vttabletMaxConnections = availableConnections
	}
	if vttabletMaxConnections <= 0 {
		log.Warningf("No available connections for vttablet after safety buffer")
		return
	}

	//todo earayu: if current vttablet is follower/readonly node, txPoolSize should be small and oltpReadPoolSize shoule be large

	//todo earayu: consider auto ajust txPoolSize and oltpReadPoolSize based on their usage.
	// e.g. if te usage > 70% and qe usage < 30%, then we should move some connections from qe to te

	// Distribute connections between te and qe
	txPoolSize := (vttabletMaxConnections * config.TxPoolPercentage) / 100
	oltpReadPoolSize := vttabletMaxConnections - txPoolSize

	// Ensure minimum pool sizes
	if txPoolSize < config.MinTxPoolSize {
		txPoolSize = config.MinTxPoolSize
	}
	if oltpReadPoolSize < config.MinOltpReadPoolSize {
		oltpReadPoolSize = config.MinOltpReadPoolSize
	}

	// Apply new pool sizes
	psc.handlePoolSizeChange(txPoolSize, oltpReadPoolSize)
}

func (psc *PoolSizeController) handlePoolSizeChange(txPoolSize int, oltpReadPoolSize int) {
	if config.DryRun {
		log.Warning("Dry-run mode enabled, skipping pool size change")
		log.Warningf("Try to set txPoolSize=%d, oltpReadPoolSize=%d", txPoolSize, oltpReadPoolSize)
		return
	}
	psc.tsv.SetTxPoolSize(txPoolSize)
	psc.tsv.SetPoolSize(oltpReadPoolSize)
	log.Infof("Adjusted pool sizes: txPoolSize=%d, oltpReadPoolSize=%d", txPoolSize, oltpReadPoolSize)
}

// handleConnectionErrors checks if Connection_errors_max_connections has increased
// and releases idle connections if necessary.
func (psc *PoolSizeController) handleConnectionErrors(currentErrors int) {
	psc.connectionErrorsMux.Lock()
	defer psc.connectionErrorsMux.Unlock()

	if psc.prevConnErrors == -1 {
		// First time fetching the value
		psc.prevConnErrors = currentErrors
		return
	}

	if currentErrors > psc.prevConnErrors {
		log.Warningf("Detected increase in Connection_errors_max_connections: %d -> %d", psc.prevConnErrors, currentErrors)
		// Release some idle connections if our pools have idle capacity
		psc.releaseIdleConnections()
	}
	psc.prevConnErrors = currentErrors
}

// releaseIdleConnections closes some idle connections without changing pool sizes
func (psc *PoolSizeController) releaseIdleConnections() {
	if config.DryRun {
		log.Warning("Dry-run mode enabled, skipping idle connection release")
		log.Warningf("Try to release %d, %d idle connections from txEngine, queryEngine", psc.te.Available(), psc.qe.Available())
		return
	}
	// Attempt to release idle connections from te
	psc.te.CloseIdleConnections(int(psc.te.Available()))
	// Attempt to release idle connections from qe
	psc.qe.CloseIdleConnections(int(psc.qe.Available()))
	log.Infof("Released idle connections from pools due to Connection_errors_max_connections increase")
}

// getGlobalVariable retrieves the value of a MySQL global variable as an integer.
func (psc *PoolSizeController) getGlobalVariable(conn *connpool.DBConn, variable string) (int, error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)
	qr, err := conn.Exec(psc.ctx, query, 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 2 {
		return 0, fmt.Errorf("unexpected result for %s: %+v", variable, qr)
	}
	valueStr := qr.Rows[0][1].ToString()
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s value: %v", variable, err)
	}
	return value, nil
}

// getGlobalStatus retrieves the value of a MySQL global status variable as an integer.
func (psc *PoolSizeController) getGlobalStatus(conn *connpool.DBConn, status string) (int, error) {
	query := fmt.Sprintf("SHOW GLOBAL STATUS LIKE '%s'", status)
	qr, err := conn.Exec(psc.ctx, query, 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 2 {
		return 0, fmt.Errorf("unexpected result for %s: %+v", status, qr)
	}
	valueStr := qr.Rows[0][1].ToString()
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s value: %v", status, err)
	}
	return value, nil
}
