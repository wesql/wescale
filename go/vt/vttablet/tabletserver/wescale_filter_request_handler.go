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
	rule, err := TransformCreateFilterToRule(stmt)
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
	if stmt.Name == "-1" {
		return errors.New("create filter failed, please set a name")
	}
	if stmt.Pattern.Plans == "-1" {
		return errors.New("create filter failed, please set plans")
	}
	if stmt.Pattern.FullyQualifiedTableNames == "-1" {
		return errors.New("create filter failed, please set fully qualified table names")
	}
	if stmt.Priority == "-1" {
		priority, err := strconv.Atoi(stmt.Priority)
		if err != nil || priority < rules.MinPriority {
			return fmt.Errorf("create filter failed, please set a valid priority that is greater than %d", rules.MinPriority)
		}
	}
	if stmt.Action.Action == "-1" {
		return errors.New("create filter failed, please set action")
	}

	if stmt.Status == "-1" {
		stmt.Status = rules.DefaultStatus
	}

	if stmt.Description == "-1" {
		stmt.Description = ""
	}
	if stmt.Pattern.QueryRegex == "-1" {
		stmt.Pattern.QueryRegex = ""
	}
	if stmt.Pattern.RequestIPRegex == "-1" {
		stmt.Pattern.RequestIPRegex = ""
	}
	if stmt.Pattern.UserRegex == "-1" {
		stmt.Pattern.UserRegex = ""
	}
	if stmt.Pattern.LeadingCommentRegex == "-1" {
		stmt.Pattern.LeadingCommentRegex = ""
	}
	if stmt.Pattern.TrailingCommentRegex == "-1" {
		stmt.Pattern.TrailingCommentRegex = ""
	}
	if stmt.Pattern.QueryTemplate == "-1" {
		stmt.Pattern.QueryTemplate = ""
	}
	if stmt.Pattern.BindVarConds == "-1" {
		stmt.Pattern.BindVarConds = ""
	}
	if stmt.Action.ActionArgs == "-1" {
		stmt.Action.ActionArgs = ""
	}
	return nil
}

func TransformCreateFilterToRule(stmt *sqlparser.CreateWescaleFilter) (*rules.Rule, error) {

	ruleInfo := make(map[string]any)

	err := SetRuleInfoBasicInfo(ruleInfo, stmt.Name, stmt.Description, stmt.Status, stmt.Priority, true)
	if err != nil {
		return nil, err
	}

	err = SetRuleInfoPattern(ruleInfo, stmt.Pattern)
	if err != nil {
		return nil, err
	}
	err = SetRuleInfoAction(ruleInfo, stmt.Action)
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

	err = AlterRuleInfo(ruleInfo, stmt)
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

func AlterRuleInfo(ruleInfo map[string]any, stmt *sqlparser.AlterWescaleFilter) error {
	err := SetRuleInfoBasicInfo(ruleInfo, stmt.NewName, stmt.Description, stmt.Status, stmt.Priority, stmt.SetPriority)
	if err != nil {
		return err
	}
	err = SetRuleInfoPattern(ruleInfo, stmt.Pattern)
	if err != nil {
		return err
	}
	err = SetRuleInfoAction(ruleInfo, stmt.Action)
	if err != nil {
		return err
	}

	return nil
}

func (qe *QueryEngine) HandleDropFilter(stmt *sqlparser.DropWescaleFilter) (*sqltypes.Result, error) {
	query, err := customrule.GetDropByNameSQL(stmt.Name)
	if err != nil {
		return nil, err
	}
	return qe.ExecuteQuery(context.Background(), query)
}

func (qe *QueryEngine) HandleShowFilter(stmt *sqlparser.ShowWescaleFilter) (*sqltypes.Result, error) {
	var query string
	var err error
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

func CheckAndFormatActionArgs(actionType, actionArgs string) (string, error) {
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

	default:
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

func SetRuleInfoBasicInfo(ruleInfo map[string]any, name, desc, status, priority string, setPriority bool) error {
	if setPriority {
		priorityInt, err := strconv.Atoi(priority)
		if err != nil {
			return fmt.Errorf("create filter failed: priority %v can't be transformed to int", priority)
		}
		if priorityInt < rules.MinPriority {
			return fmt.Errorf("alter filter failed: priority %v is smaller than min_priority %d", priority, rules.MinPriority)
		}
		ruleInfo["Priority"] = priorityInt
	}

	if status != "-1" {
		ruleInfo["Status"] = status
	}

	if name != "-1" {
		ruleInfo["Name"] = name
	}
	if desc != "-1" {
		ruleInfo["Description"] = desc
	}

	return nil
}

func SetRuleInfoAction(ruleInfo map[string]any, stmt *sqlparser.WescaleFilterAction) error {
	if stmt.Action != "-1" {
		ruleInfo["Action"] = stmt.Action
	}

	if stmt.ActionArgs != "-1" {
		actionStr, ok := ruleInfo["Action"].(string)
		if !ok {
			return fmt.Errorf("alter filter failed: action %v is not string", ruleInfo["Action"])
		}
		actionArgs, err := CheckAndFormatActionArgs(actionStr, stmt.ActionArgs)
		if err != nil {
			return err
		}
		ruleInfo["ActionArgs"] = actionArgs
	}
	return nil
}

func SetRuleInfoPattern(ruleInfo map[string]any, stmt *sqlparser.WescaleFilterPattern) error {
	if stmt.BindVarConds != "-1" {
		if stmt.BindVarConds != "" {
			return fmt.Errorf("create filter failed: bind_var_conds %v not supportted yet", stmt.BindVarConds)
		}
	}
	if stmt.Plans != "-1" {
		plans, err := customrule.UserInputStrArrayToArray(stmt.Plans)
		if err != nil {
			return fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Plans)
		}
		ruleInfo["Plans"] = plans
	}

	if stmt.FullyQualifiedTableNames != "-1" {
		fullyQualifiedTableNames, err := customrule.UserInputStrArrayToArray(stmt.FullyQualifiedTableNames)
		if err != nil {
			return fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.FullyQualifiedTableNames)
		}
		ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames
	}

	if stmt.QueryRegex != "-1" {
		ruleInfo["Query"] = stmt.QueryRegex
	}
	if stmt.QueryTemplate != "-1" {
		ruleInfo["QueryTemplate"] = stmt.QueryTemplate
	}
	if stmt.RequestIPRegex != "-1" {
		ruleInfo["RequestIP"] = stmt.RequestIPRegex
	}
	if stmt.UserRegex != "-1" {
		ruleInfo["User"] = stmt.UserRegex
	}
	if stmt.LeadingCommentRegex != "-1" {
		ruleInfo["LeadingComment"] = stmt.LeadingCommentRegex
	}
	if stmt.TrailingCommentRegex != "-1" {
		ruleInfo["TrailingComment"] = stmt.TrailingCommentRegex
	}
	return nil
}
