package tabletserver

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/log"

	"k8s.io/apimachinery/pkg/util/json"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

// todo newborn22: relocate the package?
var (
	databaseCustomRuleDbName    = sidecardb.SidecarDBName
	databaseCustomRuleTableName = "wescale_plugin"
)

// todo newborn22: where the conn inits?
// todo newborn22: relocate the method to other struct?
func (qe *QueryEngine) executeQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	var setting pools.Setting
	conn, err := qe.conns.Get(ctx, &setting)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.Exec(ctx, query, math.MaxInt32, true)
}

func (qe *QueryEngine) HandleCreateFilter(stmt *sqlparser.CreateFilter) (*sqltypes.Result, error) {
	rule, err := TransformCreateFilterToRule(stmt)
	if err != nil {
		return nil, err
	}
	query, err := GenerateInsertStatement(rule)
	if err != nil {
		return nil, err
	}
	return qe.executeQuery(context.Background(), query)
}

func TransformCreateFilterToRule(stmt *sqlparser.CreateFilter) (*rules.Rule, error) {
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

	plans, err := userInputStrArrayToJSONArray(stmt.Pattern.Plans)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
	}
	ruleInfo["Plans"] = plans

	fullyQualifiedTableNames, err := userInputStrArrayToJSONArray(stmt.Pattern.FullyQualifiedTableNames)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.Pattern.FullyQualifiedTableNames)
	}
	ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames

	ruleInfo["Status"] = strings.ToUpper(stmt.Status)

	ruleInfo["Action"] = strings.ToUpper(stmt.Action.Action)

	// todo newborn22, now it's still json
	ruleInfo["ActionArgs"] = stmt.Action.ActionArgs

	ruleInfo["Name"] = stmt.Name
	ruleInfo["Description"] = stmt.Description
	ruleInfo["Query"] = stmt.Pattern.QueryRegex
	ruleInfo["QueryTemplate"] = stmt.Pattern.QueryTemplate
	ruleInfo["RequestIP"] = stmt.Pattern.RequestIpRegex
	ruleInfo["User"] = stmt.Pattern.UserRegex
	ruleInfo["LeadingComment"] = stmt.Pattern.LeadingCommentRegex
	ruleInfo["TrailingComment"] = stmt.Pattern.TrailingCommentRegex

	return rules.BuildQueryRule(ruleInfo)
}

// todo newborn22, relocate this function to a better package.
func getInsertSQLTemplate() string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", databaseCustomRuleDbName, databaseCustomRuleTableName)
	return "INSERT INTO " + tableSchemaName + " (`name`, `description`, `priority`, `status`, `plans`, `fully_qualified_table_names`, `query_regex`, `query_template`, `request_ip_regex`, `user_regex`, `leading_comment_regex`, `trailing_comment_regex`, `bind_var_conds`, `action`, `action_args`) VALUES (%a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a)"
}

// todo newborn22, relocate this function to a better package.
// GenerateInsertStatement returns the SQL statement to insert the rule into the database.
func GenerateInsertStatement(qr *rules.Rule) (string, error) {
	insertTemplate := getInsertSQLTemplate()
	parsed := sqlparser.BuildParsedQuery(insertTemplate,
		":name",
		":description",
		":priority",
		":status",
		":plans",
		":fully_qualified_table_names",
		":query_regex",
		":query_template",
		":request_ip_regex",
		":user_regex",
		":leading_comment_regex",
		":trailing_comment_regex",
		":bind_var_conds",
		":action",
		":action_args",
	)
	bindVars, err := qr.ToBindVariable()
	if err != nil {
		return "", err
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	return bound, err
}

// todo newborn22, relocate this function to a better package.
func unmarshalArray(rawData string) ([]any, error) {
	result := make([]any, 0)
	err := json.Unmarshal([]byte(rawData), &result)
	return result, err
}

func userInputStrArrayToJSONArray(userInputArrayStr string) ([]any, error) {
	reg, _ := regexp.Compile(`\s+`)
	userInputArrayStr = reg.ReplaceAllString(userInputArrayStr, "")
	userInputArray := strings.Split(userInputArrayStr, ",")
	jsonArrayStr := "["
	first := true
	for _, s := range userInputArray {
		if !first {
			jsonArrayStr += ","
		}
		first = false
		jsonArrayStr += fmt.Sprintf("\"%s\"", s)
	}
	jsonArrayStr += "]"
	return unmarshalArray(jsonArrayStr)
}

func getSelectByNameSQL(name string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s where `name` = %%a", databaseCustomRuleDbName, databaseCustomRuleTableName)
	return sqlparser.ParseAndBind(query, sqltypes.StringBindVariable(name))
}

// todo newborn22, relocate this function to a better package.
func queryResultToRuleInfo(row sqltypes.RowNamedValues) (map[string]any, error) {
	ruleInfo := make(map[string]any)
	ruleInfo["Name"] = row.AsString("name", "")
	ruleInfo["Description"] = row.AsString("description", "")
	ruleInfo["Priority"] = int(row.AsInt64("priority", 1000))
	ruleInfo["Status"] = row.AsString("status", "")

	// parse Plans
	plansStringData := row.AsString("plans", "")
	if plansStringData != "" {
		plans, err := unmarshalArray(plansStringData)
		if err != nil {
			log.Errorf("Failed to unmarshal plans: %v", err)
			return nil, err
		}
		ruleInfo["Plans"] = plans
	}

	// parse TableNames
	tableNamesData := row.AsString("fully_qualified_table_names", "")
	if tableNamesData != "" {
		tables, err := unmarshalArray(tableNamesData)
		if err != nil {
			log.Errorf("Failed to unmarshal fully_qualified_table_names: %v", err)
			return nil, err
		}
		ruleInfo["FullyQualifiedTableNames"] = tables
	}

	ruleInfo["Query"] = row.AsString("query_regex", "")
	ruleInfo["QueryTemplate"] = row.AsString("query_template", "")
	ruleInfo["RequestIP"] = row.AsString("request_ip_regex", "")
	ruleInfo["User"] = row.AsString("user_regex", "")
	ruleInfo["LeadingComment"] = row.AsString("leading_comment_regex", "")
	ruleInfo["TrailingComment"] = row.AsString("trailing_comment_regex", "")

	// parse BindVarConds
	bindVarCondsData := row.AsString("bind_var_conds", "")
	if bindVarCondsData != "" {
		bindVarConds, err := unmarshalArray(bindVarCondsData)
		if err != nil {
			log.Errorf("Failed to unmarshal bind_var_conds: %v", err)
			return nil, err
		}
		ruleInfo["BindVarConds"] = bindVarConds
	}

	ruleInfo["Action"] = row.AsString("action", "")
	ruleInfo["ActionArgs"] = row.AsString("action_args", "")

	return ruleInfo, nil
}

func AlterRuleInfo(ruleInfo map[string]any, stmt *sqlparser.AlterFilter) error {
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
		plans, err := userInputStrArrayToJSONArray(stmt.Pattern.Plans)
		if err != nil {
			return fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
		}
		ruleInfo["Plans"] = plans
	}

	if stmt.Pattern.FullyQualifiedTableNames != "-1" {
		fullyQualifiedTableNames, err := userInputStrArrayToJSONArray(stmt.Pattern.FullyQualifiedTableNames)
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

	// todo newborn22, now it's still json
	if stmt.Action.ActionArgs != "-1" {
		ruleInfo["ActionArgs"] = stmt.Action.ActionArgs
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
	if stmt.Pattern.RequestIpRegex != "-1" {
		ruleInfo["RequestIP"] = stmt.Pattern.RequestIpRegex
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

func getUpdateSQLTemplate() string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", databaseCustomRuleDbName, databaseCustomRuleTableName)
	return "UPDATE " + tableSchemaName + " SET `name`=%a, `description`=%a, `priority`=%a, `status`=%a, `plans`=%a, `fully_qualified_table_names`=%a, `query_regex`=%a, `query_template`=%a, `request_ip_regex`=%a, `user_regex`=%a, `leading_comment_regex`=%a, `trailing_comment_regex`=%a, `bind_var_conds`=%a, `action`=%a, `action_args`=%a"
}

func GenerateUpdateStatement(qr *rules.Rule, originName string) (string, error) {
	updateTemplate := getUpdateSQLTemplate()
	parsed := sqlparser.BuildParsedQuery(updateTemplate,
		":name",
		":description",
		":priority",
		":status",
		":plans",
		":fully_qualified_table_names",
		":query_regex",
		":query_template",
		":request_ip_regex",
		":user_regex",
		":leading_comment_regex",
		":trailing_comment_regex",
		":bind_var_conds",
		":action",
		":action_args",
	)
	bindVars, err := qr.ToBindVariable()
	if err != nil {
		return "", err
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	bound = bound + fmt.Sprintf(" WHERE `name` = '%s'", originName)
	return bound, err
}

func (qe *QueryEngine) HandleAlterFilter(stmt *sqlparser.AlterFilter) (*sqltypes.Result, error) {
	query, err := getSelectByNameSQL(stmt.OriginName)
	if err != nil {
		return nil, err
	}

	qr, err := qe.executeQuery(context.Background(), query)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("the filter %s doesn't exist", stmt.OriginName)
	}

	ruleInfo, err := queryResultToRuleInfo(qr.Named().Rows[0])
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

	query, err = GenerateUpdateStatement(newFilter, stmt.OriginName)
	if err != nil {
		return nil, err
	}

	return qe.executeQuery(context.Background(), query)
}
