package customrule

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func getInsertSQLTemplate(ifNotExist bool) string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
	prefix := "INSERT "
	if ifNotExist {
		prefix += "IGNORE "
	}
	return prefix + "INTO " + tableSchemaName + " (`name`, `description`, `priority`, `status`, `plans`, `fully_qualified_table_names`, `query_regex`, `query_template`, `request_ip_regex`, `user_regex`, `leading_comment_regex`, `trailing_comment_regex`, `bind_var_conds`, `action`, `action_args`) VALUES (%a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a)"
}

// GenerateInsertStatement returns the SQL statement to insert the rule into the database.
func GenerateInsertStatement(qr *rules.Rule, ifNotExist bool) (string, error) {
	insertTemplate := getInsertSQLTemplate(ifNotExist)
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

func unmarshalArray(rawData string) ([]any, error) {
	result := make([]any, 0)
	err := json.Unmarshal([]byte(rawData), &result)
	return result, err
}

func UserInputStrArrayToArray(userInputArrayStr string) ([]any, error) {
	if userInputArrayStr == "" {
		return unmarshalArray("[]")
	}
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

func GetSelectByNameSQL(name string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s where `name` = %%a", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
	return sqlparser.ParseAndBind(query, sqltypes.StringBindVariable(name))
}

func GetSelectByActionArgsSQL(actionArgs string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s where `action_args` = %%a", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
	return sqlparser.ParseAndBind(query, sqltypes.StringBindVariable(actionArgs))
}

func GetSelectAllSQL() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
}

func GetDropByNameSQL(name string) (string, error) {
	query := fmt.Sprintf("DELETE FROM %s.%s where `name` = %%a", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
	return sqlparser.ParseAndBind(query, sqltypes.StringBindVariable(name))
}

// QueryResultToRule converts a query result to a rule.
func QueryResultToRule(row sqltypes.RowNamedValues) (*rules.Rule, error) {
	ruleInfo, err := QueryResultToRuleInfo(row)
	if err != nil {
		return nil, err
	}
	rule, err := rules.BuildQueryRule(ruleInfo)
	if err != nil {
		log.Errorf("Failed to build rule: %v", err)
		return nil, err
	}

	return rule, nil
}

func QueryResultToCreateFilter(row sqltypes.RowNamedValues) (*sqlparser.CreateWescaleFilter, error) {
	rst := &sqlparser.CreateWescaleFilter{}

	rst.Name = row.AsString("name", "")
	rst.Description = row.AsString("description", "")
	rst.Priority = strconv.Itoa(int(row.AsInt64("priority", 1000)))
	rst.Status = row.AsString("status", "")

	// parse Plans, remove `[` , `]` and `"` surrounds array elements
	plans := row.AsString("plans", "")
	plans = plans[1 : len(plans)-1]
	plans = strings.ReplaceAll(plans, `"`, "")
	rst.Pattern = &sqlparser.WescaleFilterPattern{}
	rst.Pattern.Plans = plans

	// parse TableNames, remove `[` , `]` and `"` surrounds array elements
	tableNames := row.AsString("fully_qualified_table_names", "")
	tableNames = tableNames[1 : len(tableNames)-1]
	tableNames = strings.ReplaceAll(tableNames, `"`, "")
	rst.Pattern.FullyQualifiedTableNames = tableNames

	rst.Pattern.QueryRegex = row.AsString("query_regex", "")
	rst.Pattern.QueryTemplate = row.AsString("query_template", "")
	rst.Pattern.RequestIPRegex = row.AsString("request_ip_regex", "")
	rst.Pattern.UserRegex = row.AsString("user_regex", "")
	rst.Pattern.LeadingCommentRegex = row.AsString("leading_comment_regex", "")
	rst.Pattern.TrailingCommentRegex = row.AsString("trailing_comment_regex", "")

	// BindVarConds
	// todo after supporting bind_var_conds, check the code follow:
	// BindVarConds should be ""
	rst.Pattern.BindVarConds = row.AsString("bind_var_conds", "")
	if rst.Pattern.BindVarConds != "" {
		return nil, fmt.Errorf("bind_var_conds is not supported yet, got %s", rst.Pattern.BindVarConds)
	}

	rst.Action = &sqlparser.WescaleFilterAction{}
	rst.Action.Action = row.AsString("action", "")
	rst.Action.ActionArgs = row.AsString("action_args", "")

	return rst, nil
}

func QueryResultToRuleInfo(row sqltypes.RowNamedValues) (map[string]any, error) {
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

func GetUpdateSQLTemplate() string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", DatabaseCustomRuleDbName, DatabaseCustomRuleTableName)
	return "UPDATE " + tableSchemaName + " SET `name`=%a, `description`=%a, `priority`=%a, `status`=%a, `plans`=%a, `fully_qualified_table_names`=%a, `query_regex`=%a, `query_template`=%a, `request_ip_regex`=%a, `user_regex`=%a, `leading_comment_regex`=%a, `trailing_comment_regex`=%a, `bind_var_conds`=%a, `action`=%a, `action_args`=%a"
}

func GenerateUpdateStatement(qr *rules.Rule, originName string) (string, error) {
	updateTemplate := GetUpdateSQLTemplate()
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
