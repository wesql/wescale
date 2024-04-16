package tabletserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	databaseCustomRuleDbName    = sidecardb.SidecarDBName
	databaseCustomRuleTableName = "wescale_plugin"
)

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
	if !IsCreateFilterParamValid(stmt) {
		return nil, errors.New("create filter failed, parameters are not valid")
	}
	rule, err := TransformCreateFilterToRule(stmt)
	if err != nil {
		return nil, err
	}
	query, err := GenerateInsertStatement(rule)
	if err != nil {
		return nil, err
	}
	// todo newborn22, get a conn to send to mysql.
	return qe.executeQuery(context.Background(), query)
}

// todo newborn22 make it complete
func IsCreateFilterParamValid(stmt *sqlparser.CreateFilter) bool {
	return true
}

func TransformCreateFilterToRule(stmt *sqlparser.CreateFilter) (*rules.Rule, error) {
	// todo newborn22 change user input: make user input more simple

	priority, err := strconv.Atoi(stmt.Priority)
	if err != nil {
		return nil, err
	}

	ruleInfo := make(map[string]any)
	ruleInfo["Name"] = stmt.Name
	ruleInfo["Description"] = stmt.Description
	ruleInfo["Priority"] = priority
	ruleInfo["Status"] = stmt.Status

	ruleInfo["Plans"] = []any{"Select"}
	ruleInfo["FullyQualifiedTableNames"] = []any{"d1.t1"}
	ruleInfo["Query"] = stmt.Pattern.QueryRegex
	ruleInfo["QueryTemplate"] = stmt.Pattern.QueryTemplate
	ruleInfo["RequestIP"] = stmt.Pattern.RequestIpRegex
	ruleInfo["User"] = stmt.Pattern.UserRegex
	ruleInfo["LeadingComment"] = stmt.Pattern.LeadingCommentRegex
	ruleInfo["TrailingComment"] = stmt.Pattern.TrailingCommentRegex

	ruleInfo["Action"] = stmt.Action.Action
	ruleInfo["ActionArgs"] = stmt.Action.ActionArgs

	return rules.BuildQueryRule(ruleInfo)
}

func unmarshalArray(rawData string) ([]any, error) {
	result := make([]any, 0)
	err := json.Unmarshal([]byte(rawData), &result)
	return result, err
}

// todo newborn22, relocate this function to a better package.
func getInsertSQLTemplate() string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", databaseCustomRuleDbName, databaseCustomRuleTableName)
	return "INSERT INTO " + tableSchemaName + " (`name`, `description`, `priority`, `status`, `plans`, `fully_qualified_table_names`, `query_regex`, `query_template`, `request_ip_regex`, `user_regex`, `leading_comment_regex`, `trailing_comment_regex`, `bind_var_conds`, `action`, `action_args`) VALUES (%a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a)"
}

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
