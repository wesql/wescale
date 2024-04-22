package tabletserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"

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

func TransformCreateFilterToRule(stmt *sqlparser.CreateWescaleFilter) (*rules.Rule, error) {
	if stmt.Pattern.BindVarConds != "" {
		return nil, fmt.Errorf("create filter failed: bind_var_conds %v not supportted yet", stmt.Pattern.BindVarConds)
	}

	// convert user input to internal format
	ruleInfo := make(map[string]any)

	priority, err := strconv.Atoi(stmt.Priority)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: priority %v can't be transformed to int", stmt.Priority)
	}
	ruleInfo["Priority"] = priority

	plans, err := customrule.UserInputStrArrayToJSONArray(stmt.Pattern.Plans)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
	}
	ruleInfo["Plans"] = plans

	fullyQualifiedTableNames, err := customrule.UserInputStrArrayToJSONArray(stmt.Pattern.FullyQualifiedTableNames)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.Pattern.FullyQualifiedTableNames)
	}
	ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames

	ruleInfo["Status"] = strings.ToUpper(stmt.Status)

	ruleInfo["Action"] = strings.ToUpper(stmt.Action.Action)

	actionArgs, err := UserActionArgsToJSON(ruleInfo["Action"].(string), stmt.Action.ActionArgs)
	if err != nil {
		return nil, err
	}
	ruleInfo["ActionArgs"] = actionArgs

	ruleInfo["Name"] = stmt.Name
	ruleInfo["Description"] = stmt.Description
	ruleInfo["Query"] = stmt.Pattern.QueryRegex
	ruleInfo["QueryTemplate"] = stmt.Pattern.QueryTemplate
	ruleInfo["RequestIP"] = stmt.Pattern.RequestIPRegex
	ruleInfo["User"] = stmt.Pattern.UserRegex
	ruleInfo["LeadingComment"] = stmt.Pattern.LeadingCommentRegex
	ruleInfo["TrailingComment"] = stmt.Pattern.TrailingCommentRegex

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
	if stmt.Pattern.BindVarConds != "-1" {
		if stmt.Pattern.BindVarConds != "" {
			return fmt.Errorf("create filter failed: bind_var_conds %v not supportted yet", stmt.Pattern.BindVarConds)
		}
	}

	if stmt.SetPriority {
		priority, err := strconv.Atoi(stmt.Priority)
		if err != nil {
			return fmt.Errorf("create filter failed: priority %v can't be transformed to int", stmt.Priority)
		}
		ruleInfo["Priority"] = priority
	}

	if stmt.Pattern.Plans != "-1" {
		plans, err := customrule.UserInputStrArrayToJSONArray(stmt.Pattern.Plans)
		if err != nil {
			return fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
		}
		ruleInfo["Plans"] = plans
	}

	if stmt.Pattern.FullyQualifiedTableNames != "-1" {
		fullyQualifiedTableNames, err := customrule.UserInputStrArrayToJSONArray(stmt.Pattern.FullyQualifiedTableNames)
		if err != nil {
			return fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.Pattern.FullyQualifiedTableNames)
		}
		ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames
	}

	if stmt.Status != "-1" {
		ruleInfo["Status"] = strings.ToUpper(stmt.Status)
	}

	if stmt.Action.Action != "-1" {
		ruleInfo["Action"] = strings.ToUpper(stmt.Action.Action)
	}

	if stmt.Action.ActionArgs != "-1" {
		actionArgs, err := UserActionArgsToJSON(ruleInfo["Action"].(string), stmt.Action.ActionArgs)
		if err != nil {
			return err
		}
		ruleInfo["ActionArgs"] = actionArgs
	}

	if stmt.NewName != "-1" {
		ruleInfo["Name"] = stmt.NewName
	}
	if stmt.Description != "-1" {
		ruleInfo["Description"] = stmt.Description
	}
	if stmt.Pattern.QueryRegex != "-1" {
		ruleInfo["Query"] = stmt.Pattern.QueryRegex
	}
	if stmt.Pattern.QueryTemplate != "-1" {
		ruleInfo["QueryTemplate"] = stmt.Pattern.QueryTemplate
	}
	if stmt.Pattern.RequestIPRegex != "-1" {
		ruleInfo["RequestIP"] = stmt.Pattern.RequestIPRegex
	}
	if stmt.Pattern.UserRegex != "-1" {
		ruleInfo["User"] = stmt.Pattern.UserRegex
	}
	if stmt.Pattern.LeadingCommentRegex != "-1" {
		ruleInfo["LeadingComment"] = stmt.Pattern.LeadingCommentRegex
	}
	if stmt.Pattern.TrailingCommentRegex != "-1" {
		ruleInfo["TrailingComment"] = stmt.Pattern.TrailingCommentRegex
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

// ConvertSemicolonToNewline converts ';' inside '\” to '\n' in the input string
func ConvertSemicolonToNewline(input string) string {
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

func TOMLToJSON(data string, s any) (string, error) {
	_, err := toml.Decode(data, s)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(bytes), nil

}

func UserActionArgsToJSON(actionType, actionArgs string) (string, error) {
	// user input use ';' instead of '\n' to separate key value pairs
	action, err := rules.ParseStringToAction(actionType)
	if err != nil {
		return "", err
	}

	actionArgsTOML := ConvertSemicolonToNewline(actionArgs)
	switch action {
	case rules.QRConcurrencyControl:
		type ConcurrencyControlActionArgs struct {
			MaxQueueSize   int `json:"max_queue_size" toml:"max_queue_size"`
			MaxConcurrency int `json:"max_concurrency" toml:"max_concurrency"`
		}
		ccl := &ConcurrencyControlActionArgs{}
		return TOMLToJSON(actionArgsTOML, ccl)

		//todo newborn22: implement this
		//cc := &ConcurrencyControlAction{}
		//// format pretty
		//err := cc.ParseParams(actionArgs)
		//return actionArgs, err
	default:
		return "", nil
	}
}
