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

//todo earayu: fix testcase nil pointer

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
	mockTe := mockTsv.te
	mockQe := mockTsv.qe
	mockTaskPool := background.NewTaskPool(mockTsv)

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
	psc := NewPoolSizeController(mockTsv, mockTaskPool, mockTe, mockQe)
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

func TestPoolSizeController_MinimumPoolSizeScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)
	mockTe := mockTsv.te
	mockQe := mockTsv.qe
	mockTaskPool := background.NewTaskPool(mockTsv)

	// Given
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"max_connections|20", // Very small max_connections
	))
	db.AddQuery("SHOW GLOBAL STATUS LIKE 'Connection_errors_max_connections'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Connection_errors_max_connections|0",
	))

	config.Enable = true
	config.DryRun = false
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 5
	config.TxPoolPercentage = 50
	config.MinTxPoolSize = 5
	config.MinOltpReadPoolSize = 5

	// When
	psc := NewPoolSizeController(mockTsv, mockTaskPool, mockTe, mockQe)
	psc.Reconcile()

	// Then
	// Should use minimum values since calculated values would be too small
	if mockTsv.TxPoolSize() != config.MinTxPoolSize {
		t.Errorf("TxPoolSize = %d; want %d", mockTsv.TxPoolSize(), config.MinTxPoolSize)
	}
	if mockTsv.PoolSize() != config.MinOltpReadPoolSize {
		t.Errorf("OltpReadPoolSize = %d; want %d", mockTsv.PoolSize(), config.MinOltpReadPoolSize)
	}
}

func TestPoolSizeController_DryRunScenario(t *testing.T) {
	// Mock TabletServer
	db := setUpQueryExecutorTest(t)
	mockTsv := newTestTabletServer(nil, noFlags, db)
	mockTe := mockTsv.te
	mockQe := mockTsv.qe
	mockTaskPool := background.NewTaskPool(mockTsv)

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
	config.DryRun = true // Enable dry run mode
	config.PercentageOfMaxConnections = 80
	config.SafetyBuffer = 35
	config.TxPoolPercentage = 50

	initialTxPool := mockTsv.TxPoolSize()
	initialOltpRead := mockTsv.PoolSize()

	// When
	psc := NewPoolSizeController(mockTsv, mockTaskPool, mockTe, mockQe)
	psc.Reconcile()

	// Then
	// In dry-run mode, pool sizes should not change
	if mockTsv.TxPoolSize() != initialTxPool {
		t.Errorf("TxPoolSize changed in dry-run mode: got %d; want %d", mockTsv.TxPoolSize(), initialTxPool)
	}
	if mockTsv.PoolSize() != initialOltpRead {
		t.Errorf("OltpReadPoolSize changed in dry-run mode: got %d; want %d", mockTsv.PoolSize(), initialOltpRead)
	}
}
