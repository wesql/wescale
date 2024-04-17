package tabletserver

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

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

	//userInputPlans := stmt.Pattern.Plans
	//userInputPlans = fmt.Sprintf("[%s]", userInputPlans)
	//plans, err := unmarshalArray(userInputPlans)
	//if err != nil {
	//	return nil, fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
	//}
	//reg, _ := regexp.Compile("\\s+")
	//userInputPlans = reg.ReplaceAllString(userInputPlans, "")
	//plans := strings.Split(userInputPlans, ",")
	//for _, plan := range plans {
	//	_, ok := planbuilder.PlanByNameIC(plan)
	//	if !ok {
	//		return nil, fmt.Errorf("create filter failed: plan %v is not supportted", plan)
	//	}
	//}
	plans, err := userInputStrArrayToJSONArray(stmt.Pattern.Plans)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: plans %v can't be transformed to array", stmt.Pattern.Plans)
	}
	ruleInfo["Plans"] = plans

	//userInputFullyQualifiedTableNames := stmt.Pattern.FullyQualifiedTableNames
	//userInputFullyQualifiedTableNames = fmt.Sprintf("[%s]", userInputFullyQualifiedTableNames)
	//fullyQualifiedTableNames, err := unmarshalArray(userInputFullyQualifiedTableNames)
	//if err != nil {
	//	return nil, fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.Pattern.FullyQualifiedTableNames)
	//}
	fullyQualifiedTableNames, err := userInputStrArrayToJSONArray(stmt.Pattern.FullyQualifiedTableNames)
	if err != nil {
		return nil, fmt.Errorf("create filter failed: fully_qualified_table_names %v can't be transformed to array", stmt.Pattern.FullyQualifiedTableNames)
	}
	ruleInfo["FullyQualifiedTableNames"] = fullyQualifiedTableNames

	//userInputFullyQualifiedTableNames = reg.ReplaceAllString(userInputFullyQualifiedTableNames, "")
	//ruleInfo["FullyQualifiedTableNames"] = strings.Split(userInputFullyQualifiedTableNames, ",")

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
