package tabletserver

import (
	"testing"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/background"
)

func TestValidateQueryServerPoolAutoScaleConfig(t *testing.T) {
	tests := []struct {
		name              string
		inputConfig       QueryServerPoolAutoScaleConfig
		useDefaultOnError bool
		wantErrors        int
		wantConfig        QueryServerPoolAutoScaleConfig
	}{
		{
			name: "valid config",
			inputConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 80,
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
			useDefaultOnError: false,
			wantErrors:        0,
			wantConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 80,
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
		},
		{
			name: "invalid percentage with useDefaultOnError=true",
			inputConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 95, // invalid
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
			useDefaultOnError: true,
			wantErrors:        1,
			wantConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: defaultConfig.PercentageOfMaxConnections, // should use default
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
		},
		{
			name: "invalid percentage with useDefaultOnError=false",
			inputConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 95, // invalid
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
			useDefaultOnError: false,
			wantErrors:        1,
			wantConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 95, // should remain invalid
				SafetyBuffer:               35,
				TxPoolPercentage:           50,
				MinTxPoolSize:              5,
				MinOltpReadPoolSize:        5,
			},
		},
		{
			name: "multiple invalid values",
			inputConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: 95,  // invalid
				SafetyBuffer:               -1,  // invalid
				TxPoolPercentage:           150, // invalid
				MinTxPoolSize:              -5,  // invalid
				MinOltpReadPoolSize:        -5,  // invalid
			},
			useDefaultOnError: true,
			wantErrors:        5,
			wantConfig: QueryServerPoolAutoScaleConfig{
				PercentageOfMaxConnections: defaultConfig.PercentageOfMaxConnections,
				SafetyBuffer:               defaultConfig.SafetyBuffer,
				TxPoolPercentage:           defaultConfig.TxPoolPercentage,
				MinTxPoolSize:              defaultConfig.MinTxPoolSize,
				MinOltpReadPoolSize:        defaultConfig.MinOltpReadPoolSize,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original config
			originalConfig := config

			// Set test config
			config = tt.inputConfig

			// Run validation
			errors := ValidateQueryServerPoolAutoScaleConfig(tt.useDefaultOnError)

			// Check number of errors
			if got := len(errors); got != tt.wantErrors {
				t.Errorf("ValidateQueryServerPoolAutoScaleConfig() got %v errors, want %v", got, tt.wantErrors)
			}

			// Check final config values
			if config.PercentageOfMaxConnections != tt.wantConfig.PercentageOfMaxConnections {
				t.Errorf("PercentageOfMaxConnections = %v, want %v", config.PercentageOfMaxConnections, tt.wantConfig.PercentageOfMaxConnections)
			}
			if config.SafetyBuffer != tt.wantConfig.SafetyBuffer {
				t.Errorf("SafetyBuffer = %v, want %v", config.SafetyBuffer, tt.wantConfig.SafetyBuffer)
			}
			if config.TxPoolPercentage != tt.wantConfig.TxPoolPercentage {
				t.Errorf("TxPoolPercentage = %v, want %v", config.TxPoolPercentage, tt.wantConfig.TxPoolPercentage)
			}
			if config.MinTxPoolSize != tt.wantConfig.MinTxPoolSize {
				t.Errorf("MinTxPoolSize = %v, want %v", config.MinTxPoolSize, tt.wantConfig.MinTxPoolSize)
			}
			if config.MinOltpReadPoolSize != tt.wantConfig.MinOltpReadPoolSize {
				t.Errorf("MinOltpReadPoolSize = %v, want %v", config.MinOltpReadPoolSize, tt.wantConfig.MinOltpReadPoolSize)
			}

			// Restore original config
			config = originalConfig
		})
	}
}

func TestPoolSizeControllerLifecycle(t *testing.T) {
	mockTsv := &TabletServer{}
	mockTaskPool := &background.TaskPool{}
	mockTe := &TxEngine{}
	mockQe := &QueryEngine{}

	psc := NewPoolSizeController(mockTsv, mockTaskPool, mockTe, mockQe)

	// Test Open
	if psc.isOpen.Load() {
		t.Error("Controller should not be open initially")
	}

	psc.Open()
	if !psc.isOpen.Load() {
		t.Error("Controller should be open after Open()")
	}

	// Test double Open
	psc.Open()
	if !psc.isOpen.Load() {
		t.Error("Controller should remain open after second Open()")
	}

	// Test Close
	psc.Close()
	if psc.isOpen.Load() {
		t.Error("Controller should not be open after Close()")
	}

	// Test double Close
	psc.Close()
	if psc.isOpen.Load() {
		t.Error("Controller should remain closed after second Close()")
	}
}

func TestPoolSizeController_BasicScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|1000",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// max_connections=1000, percentage=80%, safety=35
	// available = 1000 * 80% = 800
	// txPool = 800 * 50% = 400
	// oltpRead = 800 - 400 = 400
	expectedTxPool := 400
	expectedOltpRead := 400

	if mockTsv.TxPoolSize() != expectedTxPool {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), expectedTxPool)
	}
	if mockTsv.PoolSize() != expectedOltpRead {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), expectedOltpRead)
	}
}

func TestPoolSizeController_ConnectionErrorsScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|1000",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|50", // Connection errors exist
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 70 // Reduce connection percentage
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// max_connections=1000, percentage=70%, safety=35
	// available = 1000 * 70% = 700
	// txPool = 700 * 50% = 350
	// oltpRead = 700 - 350 = 350
	expectedTxPool := 350
	expectedOltpRead := 350

	if mockTsv.TxPoolSize() != expectedTxPool {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), expectedTxPool)
	}
	if mockTsv.PoolSize() != expectedOltpRead {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), expectedOltpRead)
	}
}

func TestPoolSizeController_DryRunScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|1000",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	// Save original pool sizes
	originalTxPool := mockTsv.TxPoolSize()
	originalOltpRead := mockTsv.PoolSize()

	config.Enable = true
	config.DryRun = true // Enable DryRun mode
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// In DryRun mode, pool sizes should not change
	if mockTsv.TxPoolSize() != originalTxPool {
		t.Errorf("TxPoolSize changed in DryRun mode: got %d; want %d", mockTsv.TxPoolSize(), originalTxPool)
	}
	if mockTsv.PoolSize() != originalOltpRead {
		t.Errorf("OltpReadPoolSize changed in DryRun mode: got %d; want %d", mockTsv.PoolSize(), originalOltpRead)
	}
}

func TestPoolSizeController_DisabledScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	// Save original pool sizes
	originalTxPool := mockTsv.TxPoolSize()
	originalOltpRead := mockTsv.PoolSize()

	config.Enable = false // Disable controller
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// When disabled, pool sizes should not change
	if mockTsv.TxPoolSize() != originalTxPool {
		t.Errorf("TxPoolSize changed when disabled: got %d; want %d", mockTsv.TxPoolSize(), originalTxPool)
	}
	if mockTsv.PoolSize() != originalOltpRead {
		t.Errorf("OltpReadPoolSize changed when disabled: got %d; want %d", mockTsv.PoolSize(), originalOltpRead)
	}
}

func TestPoolSizeController_SafetyBufferExceedsMaxConnections(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|30",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	// Save original pool sizes
	originalTxPool := mockTsv.TxPoolSize()
	originalOltpRead := mockTsv.PoolSize()

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 50 // Safety buffer exceeds max connections
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// Available connections are less than or equal to 0, pool sizes should not change
	if mockTsv.TxPoolSize() != originalTxPool {
		t.Errorf("TxPoolSize changed when safety buffer exceeds max connections: got %d; want %d", mockTsv.TxPoolSize(), originalTxPool)
	}
	if mockTsv.PoolSize() != originalOltpRead {
		t.Errorf("OltpReadPoolSize changed when safety buffer exceeds max connections: got %d; want %d", mockTsv.PoolSize(), originalOltpRead)
	}
}

func TestPoolSizeController_PoolSizesBelowMinimum(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|20",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 10
	config.TxPoolPercentage = 70
	config.MinTxPoolSize = 5
	config.MinOltpReadPoolSize = 5

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// max_connections=20, percentage=80%, safety=10
	// available = min(16, 10) = 10
	// txPool = 10 * 70% = 7
	// oltpRead = 10 - 7 = 3, but less than minimum, adjust to 5
	expectedTxPool := 7
	expectedOltpRead := 5

	if mockTsv.TxPoolSize() != expectedTxPool {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), expectedTxPool)
	}
	if mockTsv.PoolSize() != expectedOltpRead {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), expectedOltpRead)
	}
}

func TestPoolSizeController_AdjustedTxPoolPercentage(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|1000",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 70 // Adjust TxPool percentage
	config.MinTxPoolSize = 5
	config.MinOltpReadPoolSize = 5

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// max_connections=1000, percentage=80%, safety=35
	// available = 800
	// txPool = 800 * 70% = 560
	// oltpRead = 800 - 560 = 240
	expectedTxPool := 560
	expectedOltpRead := 240

	if mockTsv.TxPoolSize() != expectedTxPool {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), expectedTxPool)
	}
	if mockTsv.PoolSize() != expectedOltpRead {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), expectedOltpRead)
	}
}

func TestPoolSizeController_ErrorFetchingMySQLVariables(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	// Do not add queries to simulate error fetching MySQL variables

	// Save original pool sizes
	originalTxPool := mockTsv.TxPoolSize()
	originalOltpRead := mockTsv.PoolSize()

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50
	config.MinTxPoolSize = 5
	config.MinOltpReadPoolSize = 5

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// Due to error fetching MySQL variables, pool sizes should not change
	if mockTsv.TxPoolSize() != originalTxPool {
		t.Errorf("TxPoolSize changed when error fetching MySQL variables: got %d; want %d", mockTsv.TxPoolSize(), originalTxPool)
	}
	if mockTsv.PoolSize() != originalOltpRead {
		t.Errorf("OltpReadPoolSize changed when error fetching MySQL variables: got %d; want %d", mockTsv.PoolSize(), originalOltpRead)
	}
}

func TestPoolSizeController_MaxConnectionsLessThanSafetyBuffer(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|30",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	// Save original pool sizes
	originalTxPool := mockTsv.TxPoolSize()
	originalOltpRead := mockTsv.PoolSize()

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 50 // Safety buffer exceeds max connections
	config.TxPoolPercentage = 50

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// Available connections are less than or equal to 0, pool sizes should not change
	if mockTsv.TxPoolSize() != originalTxPool {
		t.Errorf("TxPoolSize changed when safety buffer exceeds max connections: got %d; want %d", mockTsv.TxPoolSize(), originalTxPool)
	}
	if mockTsv.PoolSize() != originalOltpRead {
		t.Errorf("OltpReadPoolSize changed when safety buffer exceeds max connections: got %d; want %d", mockTsv.PoolSize(), originalOltpRead)
	}
}

func TestPoolSizeController_TxPoolPercentage100(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|1000",
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 100 // TxPool occupies all available connections
	config.MinTxPoolSize = 5
	config.MinOltpReadPoolSize = 5

	// When
	psc := mockTsv.poolSizeController
	psc.Reconcile()

	// Then
	// vttabletMaxConnections = 1000 * 80% = 800
	// availableConnections = 1000 - 35 = 965
	// vttabletMaxConnections takes 800
	// txPool = 800 * 100% = 800
	// oltpRead = 800 - 800 = 0, less than minimum, adjust to minimum 5
	expectedTxPool := 800
	expectedOltpRead := 5

	if mockTsv.TxPoolSize() != expectedTxPool {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), expectedTxPool)
	}
	if mockTsv.PoolSize() != expectedOltpRead {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), expectedOltpRead)
	}
}
