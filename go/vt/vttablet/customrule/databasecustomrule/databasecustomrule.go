package topocustomrule

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sidecardb"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	databaseCustomRuleEnable         = true
	databaseCustomRuleDbName         = sidecardb.SidecarDBName
	databaseCustomRuleTableName      = "wescale_plugin"
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
			if err := cr.reloadFromDatabase(); err != nil {
				log.Warningf("Background watch of database custom rule failed: %v", err)
			}

			if cr.stopped.Load() {
				log.Warningf("Database custom rule was terminated")
				return
			}

			log.Warningf("Sleeping for %v before trying again", databaseCustomRuleReloadInterval)
			time.Sleep(databaseCustomRuleReloadInterval)
		}
	}()
}

func (cr *databaseCustomRule) stop() {
	cr.stopped.CompareAndSwap(false, true)
}

func (cr *databaseCustomRule) apply(qr *sqltypes.Result) error {
	qrs := rules.New()
	for _, row := range qr.Named().Rows {
		ruleInfo := make(map[string]any)
		ruleInfo["Name"] = row.AsString("name", "")
		ruleInfo["Description"] = row.AsString("description", "")
		ruleInfo["Priority"] = int(row.AsInt64("priority", 1000))
		//ruleInfo["status"] = row.AsString("status", "")

		// parse Plans
		plansStringData := row.AsString("plans", "")
		if plansStringData != "" {
			plans, err := unmarshalArray(plansStringData)
			if err != nil {
				log.Errorf("Failed to unmarshal plans: %v", err)
				continue
			}
			ruleInfo["Plans"] = plans
		}

		// parse TableNames
		tableNamesData := row.AsString("tableNames", "")
		if tableNamesData != "" {
			tables, err := unmarshalArray(tableNamesData)
			if err != nil {
				log.Errorf("Failed to unmarshal plans: %v", err)
				continue
			}
			ruleInfo["TableNames"] = tables
		}

		ruleInfo["Query"] = row.AsString("sql_regex", "")
		ruleInfo["RequestIP"] = row.AsString("request_ip_regex", "")
		ruleInfo["User"] = row.AsString("user_regex", "")
		ruleInfo["LeadingComment"] = row.AsString("leadingComment_regex", "")
		ruleInfo["TrailingComment"] = row.AsString("trailingComment_regex", "")

		// parse BindVarConds
		bindVarCondsData := row.AsString("bindVarConds", "")
		if bindVarCondsData != "" {
			bindVarConds, err := unmarshalArray(bindVarCondsData)
			if err != nil {
				log.Errorf("Failed to unmarshal plans: %v", err)
				continue
			}
			ruleInfo["BindVarConds"] = bindVarConds
		}

		ruleInfo["Action"] = row.AsString("action", "")
		rule, err := rules.BuildQueryRule(ruleInfo)
		if err != nil {
			log.Errorf("Failed to build rule: %v", err)
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

func (cr *databaseCustomRule) reloadFromDatabase() error {
	conn, err := cr.controller.SchemaEngine().GetConnection(context.Background())
	if err != nil {
		return fmt.Errorf("databaseCustomRule failed to get mysql connection: %v", err)
	}

	if cr.stopped.Load() {
		// We're not interested in the result any more.
		return nil
	}

	// Fetch the custom rules from the database.
	qr, err := conn.ExecOnce(context.Background(), cr.getReloadSql(), 10000, true)
	if err != nil {
		return fmt.Errorf("databaseCustomRule failed to get custom rules: %v", err)
	}
	// iterate over the rows and apply the rules
	if err := cr.apply(qr); err != nil {
		return fmt.Errorf("databaseCustomRule failed to apply custom rules: %v", err)
	}

	return nil
}

func (cr *databaseCustomRule) getReloadSql() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", databaseCustomRuleDbName, databaseCustomRuleTableName)
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
