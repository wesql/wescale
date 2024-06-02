package tabletserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vttablet/customrule"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func (qe *QueryEngine) ExecuteQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	var setting pools.Setting
	conn, err := qe.conns.Get(ctx, &setting)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.Exec(ctx, query, math.MaxInt32, true)
}

func (qe *QueryEngine) HandleCreateFilter(stmt *sqlparser.CreateWescaleFilter) (*sqltypes.Result, error) {
	err := setDefaultValueForCreateFilter(stmt)
	if err != nil {
		return nil, err
	}
	rule, err := CheckAndTransformCreateFilterToRule(qe, stmt)
	if err != nil {
		return nil, err
	}

	query, err := customrule.GenerateInsertStatement(rule, stmt.IfNotExists)
	if err != nil {
		return nil, err
	}

	return qe.ExecuteQuery(context.Background(), query)
}

func setDefaultValueForCreateFilter(stmt *sqlparser.CreateWescaleFilter) error {
	if stmt.Name == rules.UnsetValueOfStmt {
		return errors.New("create filter failed, please set a name")
	}
	if stmt.Priority == rules.UnsetValueOfStmt {
		return fmt.Errorf("create filter failed, please set a valid priority that is greater than %d", rules.MinPriority)
	}
	if stmt.Action.Action == rules.UnsetValueOfStmt {
		return errors.New("create filter failed, please set action")
	}

	if stmt.Status == rules.UnsetValueOfStmt {
		stmt.Status = rules.DefaultStatus
	}

	if stmt.Description == rules.UnsetValueOfStmt {
		stmt.Description = ""
	}
	if stmt.Pattern.QueryRegex == rules.UnsetValueOfStmt {
		stmt.Pattern.QueryRegex = rules.DefaultRegex
	}
	if stmt.Pattern.RequestIPRegex == rules.UnsetValueOfStmt {
		stmt.Pattern.RequestIPRegex = rules.DefaultRegex
	}
	if stmt.Pattern.UserRegex == rules.UnsetValueOfStmt {
		stmt.Pattern.UserRegex = rules.DefaultRegex
	}
	if stmt.Pattern.LeadingCommentRegex == rules.UnsetValueOfStmt {
		stmt.Pattern.LeadingCommentRegex = rules.DefaultRegex
	}
	if stmt.Pattern.TrailingCommentRegex == rules.UnsetValueOfStmt {
		stmt.Pattern.TrailingCommentRegex = rules.DefaultRegex
	}
	if stmt.Pattern.QueryTemplate == rules.UnsetValueOfStmt {
		stmt.Pattern.QueryTemplate = rules.DefaultQueryTemplate
	}
	if stmt.Pattern.BindVarConds == rules.UnsetValueOfStmt {
		stmt.Pattern.BindVarConds = rules.DefaultBindVarConds
	}
	if stmt.Pattern.Plans == rules.UnsetValueOfStmt {
		stmt.Pattern.Plans = rules.DefaultPlans
	}
	if stmt.Pattern.FullyQualifiedTableNames == rules.UnsetValueOfStmt {
		stmt.Pattern.FullyQualifiedTableNames = rules.DefaultFullyQualifiedTableNames
	}
	if stmt.Action.ActionArgs == rules.UnsetValueOfStmt {
		stmt.Action.ActionArgs = rules.DefaultActionArgs
	}
	return nil
}

func CheckAndTransformCreateFilterToRule(qe *QueryEngine, stmt *sqlparser.CreateWescaleFilter) (*rules.Rule, error) {

	ruleInfo := make(map[string]any)

	err := CheckAndSetRuleInfoBasicInfo(ruleInfo, stmt.Name, stmt.Description, stmt.Status, stmt.Priority)
	if err != nil {
		return nil, err
	}

	err = CheckAndSetRuleInfoPattern(ruleInfo, stmt.Pattern)
	if err != nil {
		return nil, err
	}
	err = CheckAndSetRuleInfoAction(qe, ruleInfo, stmt.Action)
	if err != nil {
		return nil, err
	}

	return rules.BuildQueryRule(ruleInfo)
}

func (qe *QueryEngine) HandleAlterFilter(stmt *sqlparser.AlterWescaleFilter) (*sqltypes.Result, error) {
	query, err := customrule.GetSelectByNameSQL(stmt.OriginName)
	if err != nil {
		return nil, err
	}

	qr, err := qe.ExecuteQuery(context.Background(), query)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("the filter %s doesn't exist", stmt.OriginName)
	}

	ruleInfo, err := customrule.QueryResultToRuleInfo(qr.Named().Rows[0])
	if err != nil {
		return nil, err
	}

	err = CheckAndAlterRuleInfo(qe, ruleInfo, stmt)
	if err != nil {
		return nil, err
	}

	newFilter, err := rules.BuildQueryRule(ruleInfo)
	if err != nil {
		return nil, err
	}

	query, err = customrule.GenerateUpdateStatement(newFilter, stmt.OriginName)
	if err != nil {
		return nil, err
	}

	return qe.ExecuteQuery(context.Background(), query)
}

func CheckAndAlterRuleInfo(qe *QueryEngine, ruleInfo map[string]any, stmt *sqlparser.AlterWescaleFilter) error {
	err := CheckAndSetRuleInfoBasicInfo(ruleInfo, stmt.AlterInfo.Name, stmt.AlterInfo.Description, stmt.AlterInfo.Status, stmt.AlterInfo.Priority)
	if err != nil {
		return err
	}
	err = CheckAndSetRuleInfoPattern(ruleInfo, stmt.AlterInfo.Pattern)
	if err != nil {
		return err
	}
	err = CheckAndSetRuleInfoAction(qe, ruleInfo, stmt.AlterInfo.Action)
	if err != nil {
		return err
	}

	return nil
}

func (qe *QueryEngine) HandleDropFilter(stmt *sqlparser.DropWescaleFilter) (*sqltypes.Result, error) {
	selectQeury, err := customrule.GetSelectByNameSQL(stmt.Name)
	if err != nil {
		return nil, err
	}
	filterQueryRst, err := qe.ExecuteQuery(context.Background(), selectQeury)
	if err != nil {
		return nil, err
	}
	if len(filterQueryRst.Named().Rows) < 1 {
		return &sqltypes.Result{}, nil
	}

	dropQuery, err := customrule.GetDropByNameSQL(stmt.Name)
	if err != nil {
		return nil, err
	}
	rst, err := qe.ExecuteQuery(context.Background(), dropQuery)
	if err != nil {
		return nil, err
	}

	filerName := filterQueryRst.Named().Rows[0].AsString("name", "")
	filterAction, _ := rules.ParseStringToAction(filterQueryRst.Named().Rows[0].AsString("action", ""))
	qe.removeFilterMemoryData(filerName, filterAction)

	return rst, nil
}

func (qe *QueryEngine) removeFilterMemoryData(name string, action rules.Action) {
	switch action {
	case rules.QRWasmPlugin:
		qe.wasmPluginController.VM.ClearWasmModule(name)

	case rules.QRConcurrencyControl:
		qe.concurrencyController.DeleteQueue(name)
	}
}

func (qe *QueryEngine) HandleShowFilter(stmt *sqlparser.ShowWescaleFilter) (*sqltypes.Result, error) {
	var query string
	var err error
	if stmt.ShowCreate {
		return qe.HandleShowCreateFilter(stmt)
	}

	if stmt.ShowAll {
		query = customrule.GetSelectAllSQL()
	} else {
		query, err = customrule.GetSelectByNameSQL(stmt.Name)
		if err != nil {
			return nil, err
		}
	}
	return qe.ExecuteQuery(context.Background(), query)
}

func (qe *QueryEngine) HandleShowCreateFilter(stmt *sqlparser.ShowWescaleFilter) (*sqltypes.Result, error) {
	query, err := customrule.GetSelectByNameSQL(stmt.Name)
	if err != nil {
		return nil, err
	}
	qr, err := qe.ExecuteQuery(context.Background(), query)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("the expected filter num is 1 but got %d", len(qr.Named().Rows))
	}
	createFilter, err := customrule.QueryResultToCreateFilter(qr.Named().Rows[0])
	if err != nil {
		return nil, err
	}
	rows := [][]sqltypes.Value{}
	rows = append(rows, BuildVarCharRow(
		stmt.Name,
		sqlparser.String(createFilter),
	))
	return &sqltypes.Result{
		Fields: BuildVarCharFields("Filer", "Create Filter"),
		Rows:   rows,
	}, nil
}

// ConvertUserInputToTOML converts ';' inside '\” to '\n' in the input string
func ConvertUserInputToTOML(input string) string {
	var result strings.Builder
	insideQuotes := false

	for _, char := range input {
		if char == '"' {
			insideQuotes = !insideQuotes
		}
		if char == ';' && !insideQuotes {
			result.WriteString("\n")
		} else {
			result.WriteRune(char)
		}
	}

	return result.String()
}

func CheckAndFormatActionArgs(qe *QueryEngine, filerName, actionType, actionArgs string) (string, error) {

	action, err := rules.ParseStringToAction(actionType)
	if err != nil {
		return "", err
	}

	switch action {
	case rules.QRConcurrencyControl:
		ccl := &ConcurrencyControlActionArgs{}
		_, err := ccl.Parse(actionArgs)
		if err != nil {
			return "", err
		}
		return FormatUserInputStr(actionArgs), nil

	case rules.QRWasmPlugin:
		plugin := &WasmPluginActionArgs{}
		args, err := plugin.Parse(actionArgs)
		if err != nil {
			return "", err
		}
		// check bytes can be complied or not
		wasmPluginArgs, _ := args.(*WasmPluginActionArgs)
		bytes, err := qe.wasmPluginController.GetWasmBytesByBinaryName(context.Background(), wasmPluginArgs.WasmBinaryName)
		if err != nil {
			return "", err
		}
		wasmModule, err := qe.wasmPluginController.VM.CompileWasmModule(bytes)
		if err != nil {
			return "", fmt.Errorf("err when compiling wasm moulde %v", err)
		}
		// whether create or alter, just set
		qe.wasmPluginController.VM.SetWasmModule(filerName, wasmModule)

		return FormatUserInputStr(actionArgs), nil
	case rules.QRSkipFilter:
		skipFilterArgs := &SkipFilterActionArgs{}
		_, err := skipFilterArgs.Parse(actionArgs)
		if err != nil {
			return "", err
		}
		return FormatUserInputStr(actionArgs), nil
	default:
		if actionArgs != "" {
			return "", fmt.Errorf("action %v does not support action_args", action)
		}
		return "", nil
	}
}

// FormatUserInputStr remove space and tab from the input string
func FormatUserInputStr(str string) string {
	var result strings.Builder
	insideQuotes := false

	for _, char := range str {
		if char == '"' {
			insideQuotes = !insideQuotes
		}
		if (char == ' ' || char == '\t') && !insideQuotes {
			continue
		}
		result.WriteRune(char)
	}
	return result.String()
}

func CheckAndSetRuleInfoBasicInfo(ruleInfo map[string]any, name, desc, status, priority string) error {
	if priority != rules.UnsetValueOfStmt {
		priorityInt, err := strconv.Atoi(priority)
		if err != nil {
			return fmt.Errorf("create filter failed: priority %v can't be transformed to int", priority)
		}
		if priorityInt < rules.MinPriority {
			return fmt.Errorf("alter filter failed: priority %v is smaller than min_priority %d", priority, rules.MinPriority)
		}
		ruleInfo["Priority"] = priorityInt
	}

	if status != rules.UnsetValueOfStmt {
		ruleInfo["Status"] = status
	}

	if name != rules.UnsetValueOfStmt {
		ruleInfo["Name"] = name
	}
	if desc != rules.UnsetValueOfStmt {
		ruleInfo["Description"] = desc
	}

	return nil
}

func CheckAndSetRuleInfoAction(qe *QueryEngine, ruleInfo map[string]any, stmt *sqlparser.WescaleFilterAction) error {
	if stmt.Action != rules.UnsetValueOfStmt {
		ruleInfo["Action"] = stmt.Action
	}

	if stmt.ActionArgs != rules.UnsetValueOfStmt {
		actionStr, ok := ruleInfo["Action"].(string)
		if !ok {
			return fmt.Errorf("alter filter failed: action %v is not string", ruleInfo["Action"])
		}
		filerName, ok := ruleInfo["Name"].(string)
		if !ok {
			return fmt.Errorf("alter filter failed: name %v is not string", ruleInfo["Name"])
		}
		actionArgs, err := CheckAndFormatActionArgs(qe, filerName, actionStr, stmt.ActionArgs)
		if err != nil {
			return err
		}
		ruleInfo["ActionArgs"] = actionArgs
	}
	return nil
}

func CheckAndSetRuleInfoPattern(ruleInfo map[string]any, stmt *sqlparser.WescaleFilterPattern) error {
	if stmt.BindVarConds != rules.UnsetValueOfStmt {
		if stmt.BindVarConds != "" {
			return fmt.Errorf("create filter failed: bind_var_conds %v not supportted yet", stmt.BindVarConds)
		}
	}
	if stmt.Plans != rules.UnsetValueOfStmt {
		plans, err := customrule.UserInputStrArrayToArray(stmt.Plans)
		if err != nil {
			return fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Plans)
		}
		ruleInfo["Plans"] = plans
	}

	if stmt.FullyQualifiedTableNames != rules.UnsetValueOfStmt {
		fullyQualifiedTableNames, err := customrule.UserInputStrArrayToArray(stmt.FullyQualifiedTableNames)
		if err != nil {
			return fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.FullyQualifiedTableNames)
		}
		ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames
	}

	if stmt.QueryRegex != rules.UnsetValueOfStmt {
		ruleInfo["Query"] = stmt.QueryRegex
	}
	if stmt.QueryTemplate != rules.UnsetValueOfStmt {
		ruleInfo["QueryTemplate"] = stmt.QueryTemplate
	}
	if stmt.RequestIPRegex != rules.UnsetValueOfStmt {
		ruleInfo["RequestIP"] = stmt.RequestIPRegex
	}
	if stmt.UserRegex != rules.UnsetValueOfStmt {
		ruleInfo["User"] = stmt.UserRegex
	}
	if stmt.LeadingCommentRegex != rules.UnsetValueOfStmt {
		ruleInfo["LeadingComment"] = stmt.LeadingCommentRegex
	}
	if stmt.TrailingCommentRegex != rules.UnsetValueOfStmt {
		ruleInfo["TrailingComment"] = stmt.TrailingCommentRegex
	}
	return nil
}
