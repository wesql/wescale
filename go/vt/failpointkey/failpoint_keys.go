/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package failpointkey

import "github.com/pingcap/failpoint"

type FailpointValue struct {
	FullName   string
	Name       string
	ExampleStr string
}

var FailpointTable map[string]FailpointValue

var (
	TestFailPointError = FailpointValue{
		FullName:   "TestFailPointError",
		Name:       "TestFailPointError",
		ExampleStr: "",
	}
	CreateDatabaseErrorOnDbname = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname",
		Name:       "create-database-error-on-dbname",
		ExampleStr: "return(db)",
	}
	IsVReplMigrationReadyToCutOver = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vttablet/onlineddl/IsVReplMigrationReadyToCutOver",
		Name:       "IsVReplMigrationReadyToCutOver",
		ExampleStr: "return(true)",
	}
	WaitJustBeforeStopVreplication = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustBeforeStopVreplication",
		Name:       "WaitJustBeforeStopVreplication",
		ExampleStr: "return(true)",
	}
	WaitJustAfterStopVreplication = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vttablet/onlineddl/WaitJustAfterStopVreplication",
		Name:       "WaitJustAfterStopVreplication",
		ExampleStr: "return(true)",
	}
	AssertRoutingTabletType = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType",
		Name:       "AssertRoutingTabletType",
		ExampleStr: "return(\"primary\")",
	}
	ModifyBatchSize = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vttablet/jobcontroller/ModifyBatchSize",
		Name:       "ModifyBatchSize",
		ExampleStr: "return(30)",
	}
	CreateErrorWhenExecutingBatch = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vttablet/jobcontroller/CreateErrorWhenExecutingBatch",
		Name:       "CreateErrorWhenExecutingBatch",
		ExampleStr: "return(true)",
	}
	CrashVTGate = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/CrashVTGate",
		Name:       "CrashVTGate",
		ExampleStr: "return(true)",
	}
	CreateSidecarDbError = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/sidecardb/CreateSidecarDbError",
		Name:       "CreateSidecarDbError",
		ExampleStr: "return(true)",
	}
	BranchFetchSnapshotError = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/branch/BranchFetchSnapshotError",
		Name:       "BranchFetchSnapshotError",
		ExampleStr: "return(true)",
	}
	BranchApplySnapshotError = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/branch/BranchApplySnapshotError",
		Name:       "BranchApplySnapshotError",
		ExampleStr: "return(true)",
	}
	BranchInsertMergeBackDDLError = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/branch/BranchInsertMergeBackDDLError",
		Name:       "BranchInsertMergeBackDDLError",
		ExampleStr: "return(true)",
	}
	BranchExecuteMergeBackDDLError = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/branch/BranchExecuteMergeBackDDLError",
		Name:       "BranchExecuteMergeBackDDLError",
		ExampleStr: "return(true)",
	}
	VTGateExecuteInTxnRollback = FailpointValue{
		FullName:   "vitess.io/vitess/go/vt/vtgate/engine/VTGateExecuteInTxnRollback",
		Name:       "VTGateExecuteInTxnRollback",
		ExampleStr: "return(true)",
	}
)

func init() {
	err := failpoint.Enable("vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint", "return(1)")
	if err != nil {
		return
	}
	FailpointTable = make(map[string]FailpointValue)
	FailpointTable[CreateDatabaseErrorOnDbname.FullName] = CreateDatabaseErrorOnDbname
	FailpointTable[TestFailPointError.FullName] = TestFailPointError
	FailpointTable[IsVReplMigrationReadyToCutOver.FullName] = IsVReplMigrationReadyToCutOver
	FailpointTable[WaitJustBeforeStopVreplication.FullName] = WaitJustBeforeStopVreplication
	FailpointTable[WaitJustAfterStopVreplication.FullName] = WaitJustAfterStopVreplication
	FailpointTable[AssertRoutingTabletType.FullName] = AssertRoutingTabletType
	FailpointTable[ModifyBatchSize.FullName] = ModifyBatchSize
	FailpointTable[CreateErrorWhenExecutingBatch.FullName] = CreateErrorWhenExecutingBatch
	FailpointTable[CrashVTGate.FullName] = CrashVTGate
	FailpointTable[CreateSidecarDbError.FullName] = CreateSidecarDbError
	FailpointTable[BranchFetchSnapshotError.FullName] = BranchFetchSnapshotError
	FailpointTable[BranchApplySnapshotError.FullName] = BranchApplySnapshotError
	FailpointTable[BranchInsertMergeBackDDLError.FullName] = BranchInsertMergeBackDDLError
	FailpointTable[BranchExecuteMergeBackDDLError.FullName] = BranchExecuteMergeBackDDLError
	FailpointTable[VTGateExecuteInTxnRollback.FullName] = VTGateExecuteInTxnRollback
}
