package engine

import (
	"context"
	"fmt"
	"strconv"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/branch"
)

var _ Primitive = (*Branch)(nil)

type BranchCommandType string

const (
	Create           BranchCommandType = "create"
	Diff             BranchCommandType = "diff"
	MergeBack        BranchCommandType = "mergeBack"
	PrepareMergeBack BranchCommandType = "prepareMergeBack"
	Cleanup          BranchCommandType = "cleanUp"
	Show             BranchCommandType = "show"
)

const (
	DefaultBranchName = "my_branch"
)

// Branch is an operator to deal with branch commands
type Branch struct {
	// set when plan building
	name        string
	commandType BranchCommandType
	params      branchParams

	noInputs
}

type branchParams interface {
	validate() error
	setValues(params map[string]string) error
}

const (
	BranchParamsName = "name"
)

const (
	BranchCreateParamsSourceHost      = "source_host"
	BranchCreateParamsSourcePort      = "source_port"
	BranchCreateParamsSourceUser      = "source_user"
	BranchCreateParamsSourcePassword  = "source_password"
	BranchCreateParamsInclude         = "include_databases"
	BranchCreateParamsExclude         = "exclude_databases"
	BranchCreateParamsTargetDBPattern = "target_db_pattern"
)

type BranchCreateParams struct {
	SourceHost      string
	SourcePort      string
	SourceUser      string
	SourcePassword  string
	Include         string
	Exclude         string
	TargetDBPattern string
}

const (
	BranchDiffParamsCompareObjects = "compare_objects"
)

type BranchDiffParams struct {
	CompareObjects string
}

const (
	BranchPrepareMergeBackParamsMergeOption = "merge_option"
)

type BranchPrepareMergeBackParams struct {
	MergeOption string
}

const (
	BranchShowParamsShowOption = "show_option"
)

type BranchShowParams struct {
	ShowOption string
}

// ***************************************************************************************************************************************************

func BuildBranchPlan(branchCmd *sqlparser.BranchCommand) (*Branch, error) {
	// plan will be cached and index by sql template, so here we just build some static info related the sql.
	cmdType, err := parseBranchCommandType(branchCmd.Type)
	if err != nil {
		return nil, err
	}
	params := make(map[string]string)
	for i, _ := range branchCmd.Params.Keys {
		params[branchCmd.Params.Keys[i]] = branchCmd.Params.Values[i]
	}
	b := &Branch{commandType: cmdType}
	err = b.setAndValidateParams(params)
	if err != nil {
		return nil, fmt.Errorf("invalid branch command params: %w", err)
	}
	return b, nil
}

// todo complete me
// TryExecute implements Primitive interface
func (b *Branch) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	return result, nil
}

// todo complete me
// TryStreamExecute implements Primitive interface
func (b *Branch) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result := &sqltypes.Result{}
	return callback(result)
}

// GetFields implements Primitive interface
func (b *Branch) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (b *Branch) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "Branch"
	return rst
}

// NeedsTransaction implements the Primitive interface
func (b *Branch) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (b *Branch) RouteType() string {
	return "Branch"
}

// GetKeyspaceName implements Primitive interface
func (b *Branch) GetKeyspaceName() string {
	return ""
}

// GetTableName implements Primitive interface
func (b *Branch) GetTableName() string {
	return ""
}

// ***************************************************************************************************************************************************

func parseBranchCommandType(s string) (BranchCommandType, error) {
	switch s {
	case string(Create):
		return Create, nil
	case string(Diff):
		return Diff, nil
	case string(MergeBack):
		return MergeBack, nil
	case string(PrepareMergeBack):
		return PrepareMergeBack, nil
	case string(Cleanup):
		return Cleanup, nil
	case string(Show):
		return Show, nil
	default:
		return "", fmt.Errorf("invalid branch command type: %s", s)
	}
}

// todo complete me
func (b *Branch) setAndValidateParams(paramsMap map[string]string) error {
	if name, exists := paramsMap[BranchParamsName]; exists {
		b.name = name
		delete(paramsMap, BranchParamsName)
	} else {
		b.name = DefaultBranchName
	}

	var params branchParams
	switch b.commandType {
	case Create:
		params = &BranchCreateParams{}
	case Diff:
		params = &BranchDiffParams{}
	case PrepareMergeBack:
		params = &BranchPrepareMergeBackParams{}
	case Show:
		params = &BranchShowParams{}
	case MergeBack, Cleanup:
		return nil
	default:
		return fmt.Errorf("invalid branch command type: %s", b.commandType)
	}
	err := params.setValues(paramsMap)
	if err != nil {
		return err
	}
	err = params.validate()
	if err != nil {
		return err
	}
	b.params = params
	return nil
}

func checkRedundantParams(params map[string]string) error {
	if len(params) > 0 {
		invalidParams := make([]string, 0)
		for k := range params {
			invalidParams = append(invalidParams, k)
		}
		return fmt.Errorf("invalid params: %v", invalidParams)
	}
	return nil
}

func (bcp *BranchCreateParams) setValues(params map[string]string) error {
	if v, ok := params[BranchCreateParamsSourcePort]; ok {
		bcp.SourcePort = v
	} else {
		bcp.SourcePort = "3306"
		delete(params, BranchCreateParamsSourcePort)
	}

	if v, ok := params[BranchCreateParamsInclude]; ok {
		bcp.Include = v
		delete(params, BranchCreateParamsInclude)
	} else {
		bcp.Include = "*"
	}

	if v, ok := params[BranchCreateParamsExclude]; ok {
		bcp.Exclude = v
		delete(params, BranchCreateParamsExclude)
	} else {
		bcp.Exclude = "information_schema,mysql.performance_schema,sys"
	}

	if v, ok := params[BranchCreateParamsSourceHost]; ok {
		bcp.SourceHost = v
		delete(params, BranchCreateParamsSourceHost)
	}

	if v, ok := params[BranchCreateParamsSourceUser]; ok {
		bcp.SourceUser = v
		delete(params, BranchCreateParamsSourceUser)
	}

	if v, ok := params[BranchCreateParamsSourcePassword]; ok {
		bcp.SourcePassword = v
		delete(params, BranchCreateParamsSourcePassword)
	}

	if v, ok := params[BranchCreateParamsTargetDBPattern]; ok {
		bcp.TargetDBPattern = v
		delete(params, BranchCreateParamsTargetDBPattern)
	}

	return checkRedundantParams(params)
}

// todo complete me
func (bcp *BranchCreateParams) validate() error {
	if bcp.SourceHost == "" {
		return fmt.Errorf("branch create: source host is required")
	} else {
		// todo check if host is valid ip format
	}

	if bcp.SourcePort == "" {
		return fmt.Errorf("branch create: source port is required")
	} else {
		_, err := strconv.Atoi(bcp.SourcePort)
		if err != nil {
			return fmt.Errorf("branch create: source port is not a number")
		}
	}

	if bcp.Include == "" {
		return fmt.Errorf("branch create: include databases is required")
	}

	return nil
}

// todo complete me
// todo output type
func (bdp *BranchDiffParams) setValues(params map[string]string) error {
	if v, ok := params[BranchDiffParamsCompareObjects]; ok {
		bdp.CompareObjects = v
		delete(params, BranchDiffParamsCompareObjects)
	} else {
		bdp.CompareObjects = string(branch.FromSourceToTarget)
	}

	return checkRedundantParams(params)
}

// todo complete me
// todo output type
func (bdp *BranchDiffParams) validate() error {
	switch branch.BranchDiffObjectsFlag(bdp.CompareObjects) {
	case branch.FromSourceToTarget, branch.FromTargetToSource,
		branch.FromSnapshotToSource, branch.FromSnapshotToTarget,
		branch.FromTargetToSnapshot, branch.FromSourceToSnapshot:
	default:
		return fmt.Errorf("invalid compare objects: %s", bdp.CompareObjects)
	}

	return nil
}

func (bpp *BranchPrepareMergeBackParams) setValues(params map[string]string) error {
	if v, ok := params[BranchPrepareMergeBackParamsMergeOption]; ok {
		bpp.MergeOption = v
		delete(params, BranchPrepareMergeBackParamsMergeOption)
	} else {
		bpp.MergeOption = string(branch.MergeOverride)
	}

	return checkRedundantParams(params)
}

func (bpp *BranchPrepareMergeBackParams) validate() error {
	switch branch.MergeBackOption(bpp.MergeOption) {
	case branch.MergeOverride, branch.MergeDiff:
	default:
		return fmt.Errorf("invalid merge option: %s", bpp.MergeOption)
	}
	return nil
}

func (bsp *BranchShowParams) setValues(params map[string]string) error {
	if v, ok := params[BranchShowParamsShowOption]; ok {
		bsp.ShowOption = v
		delete(params, BranchShowParamsShowOption)
	} else {
		bsp.ShowOption = string(branch.ShowAll)
	}

	return checkRedundantParams(params)
}

func (bsp *BranchShowParams) validate() error {
	switch branch.BranchShowOption(bsp.ShowOption) {
	case branch.ShowAll, branch.ShowSnapshot, branch.ShowStatus:
	default:
		return fmt.Errorf("invalid merge option: %s", bsp.ShowOption)
	}
	return nil
}
