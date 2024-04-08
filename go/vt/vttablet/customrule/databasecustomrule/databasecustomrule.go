/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sidecardb"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	databaseCustomRuleEnable    = true
	databaseCustomRuleDbName    = sidecardb.SidecarDBName
	databaseCustomRuleTableName = "wescale_plugin"
	//todo filter: change the reload interval to 60s
	databaseCustomRuleReloadInterval = 1 * time.Second
)

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&databaseCustomRuleEnable, "database_custom_rule_enable", databaseCustomRuleEnable, "enable database custom rule")
	fs.StringVar(&databaseCustomRuleDbName, "database_custom_rule_db_name", databaseCustomRuleDbName, "sidecar db name for customrules file. default is mysql")
	fs.StringVar(&databaseCustomRuleTableName, "database_custom_rule_table_name", databaseCustomRuleTableName, "table name for customrules file. default is wescale_plugin")
	fs.DurationVar(&databaseCustomRuleReloadInterval, "database_custom_rule_reload_interval", databaseCustomRuleReloadInterval, "reload interval for customrules file. default is 60s")
}

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

// databaseCustomRuleSource is database based custom rule source name
const databaseCustomRuleSource string = "DATABASE_CUSTOM_RULE"

// databaseCustomRule is the database backed implementation.
type databaseCustomRule struct {
	// controller is set at construction time.
	controller tabletserver.Controller

	// qrs is the current rule set that we read.
	qrs *rules.Rules

	// stopped is set when stop() is called. It is a protection for race conditions.
	stopped atomic.Bool
}

func newDatabaseCustomRule(qsc tabletserver.Controller) (*databaseCustomRule, error) {
	return &databaseCustomRule{
		controller: qsc,
	}, nil
}

func (cr *databaseCustomRule) start() {
	go func() {
		for {
			if err := cr.reloadRulesFromDatabase(); err != nil {
				log.Warningf("Background watch of database custom rule failed: %v", err)
			}

			if cr.stopped.Load() {
				log.Warningf("Database custom rule was terminated")
				return
			}

			time.Sleep(databaseCustomRuleReloadInterval)
		}
	}()
}

func (cr *databaseCustomRule) stop() {
	cr.stopped.CompareAndSwap(false, true)
}

// queryResultToRule converts a query result to a rule.
func queryResultToRule(row sqltypes.RowNamedValues) (*rules.Rule, error) {
	ruleInfo := make(map[string]any)
	ruleInfo["Name"] = row.AsString("name", "")
	ruleInfo["Description"] = row.AsString("description", "")
	ruleInfo["Priority"] = int(row.AsInt64("priority", 1000))
	ruleInfo["Status"] = row.AsString("status", "")
	ruleInfo["FireNext"] = row.AsBool("fire_next", true)

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

	rule, err := rules.BuildQueryRule(ruleInfo)
	if err != nil {
		log.Errorf("Failed to build rule: %v", err)
		return nil, err
	}

	return rule, nil
}

func (cr *databaseCustomRule) applyRules(qr *sqltypes.Result) error {
	qrs := rules.New()
	for _, row := range qr.Named().Rows {
		if cr.stopped.Load() {
			// We're not interested in the result any more.
			return nil
		}
		rule, err := queryResultToRule(row)
		if err != nil {
			continue
		}
		qrs.Add(rule)
	}

	if !reflect.DeepEqual(cr.qrs, qrs) {
		cr.qrs = qrs.Copy()
		cr.controller.SetQueryRules(databaseCustomRuleSource, qrs)
		log.Infof("Custom rule version %v fetched from topo and applied to vttablet")
	}

	return nil
}

func (cr *databaseCustomRule) reloadRulesFromDatabase() error {
	conn, err := cr.controller.SchemaEngine().GetConnection(context.Background())
	if err != nil {
		return fmt.Errorf("databaseCustomRule failed to get mysql connection: %v", err)
	}
	defer conn.Recycle()

	// Fetch the custom rules from the database.
	qr, err := conn.ExecOnce(context.Background(), cr.getReloadSQL(), 10000, true)
	if err != nil {
		return fmt.Errorf("databaseCustomRule failed to get custom rules: %v", err)
	}
	// iterate over the rows and applyRules the rules
	if err := cr.applyRules(qr); err != nil {
		return fmt.Errorf("databaseCustomRule failed to applyRules custom rules: %v", err)
	}

	return nil
}

func (cr *databaseCustomRule) getReloadSQL() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", databaseCustomRuleDbName, databaseCustomRuleTableName)
}

func (cr *databaseCustomRule) getInsertSQLTemplate() string {
	tableSchemaName := fmt.Sprintf("`%s`.`%s`", databaseCustomRuleDbName, databaseCustomRuleTableName)
	return "INSERT INTO " + tableSchemaName + " (`name`, `description`, `priority`, `status`, `plans`, `fully_qualified_table_names`, `query_regex`, `query_template`, `request_ip_regex`, `user_regex`, `leading_comment_regex`, `trailing_comment_regex`, `bind_var_conds`, `action`, `action_args`) VALUES (%a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a)"
}

// GenerateInsertStatement returns the SQL statement to insert the rule into the database.
func (cr *databaseCustomRule) GenerateInsertStatement(qr *rules.Rule) (string, error) {
	insertTemplate := cr.getInsertSQLTemplate()
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

// activateTopoCustomRules activates database dynamic custom rule mechanism.
func activateTopoCustomRules(qsc tabletserver.Controller) {
	if databaseCustomRuleEnable {
		qsc.RegisterQueryRuleSource(databaseCustomRuleSource)

		cr, err := newDatabaseCustomRule(qsc)
		if err != nil {
			log.Fatalf("cannot start DatabaseCustomRule: %v", err)
		}
		cr.start()

		servenv.OnTerm(cr.stop)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, activateTopoCustomRules)
}
