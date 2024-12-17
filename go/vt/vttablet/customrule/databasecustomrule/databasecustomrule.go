/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"context"
	"sync"

	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/vttablet/customrule"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

// databaseCustomRuleSource is database based custom rule source name
const databaseCustomRuleSource string = "DATABASE_CUSTOM_RULE"

// databaseCustomRule is the database backed implementation.
type databaseCustomRule struct {
	mu sync.Mutex

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
		// reload rules that already in the database once start
		if err := cr.reloadRulesFromDatabase(); err != nil {
			log.Warningf("Background watch of database custom rule failed: %v", err)
		}

		intervalTicker := time.NewTicker(customrule.DatabaseCustomRuleReloadInterval)
		defer intervalTicker.Stop()

		for {
			select {
			case <-intervalTicker.C:
			case <-customrule.Watch():
			}

			if err := cr.reloadRulesFromDatabase(); err != nil {
				log.Warningf("Background watch of database custom rule failed: %v", err)
			}

			if cr.stopped.Load() {
				log.Warningf("Database custom rule was terminated")
				return
			}
		}
	}()
}

func (cr *databaseCustomRule) stop() {
	cr.stopped.CompareAndSwap(false, true)
}

func (cr *databaseCustomRule) applyRules(qr *sqltypes.Result) error {
	qrs := rules.New()
	for _, row := range qr.Named().Rows {
		if cr.stopped.Load() {
			// We're not interested in the result any more.
			return nil
		}
		rule, err := customrule.QueryResultToRule(row)
		if err != nil {
			continue
		}
		qrs.Add(rule)
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

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
	qr, err := conn.ExecOnce(context.Background(), customrule.GetSelectAllSQL(), 10000, true)
	if err != nil {
		return fmt.Errorf("databaseCustomRule failed to get custom rules: %v", err)
	}
	// iterate over the rows and applyRules the rules
	if err := cr.applyRules(qr); err != nil {
		return fmt.Errorf("databaseCustomRule failed to applyRules custom rules: %v", err)
	}

	return nil
}

// activateTopoCustomRules activates database dynamic custom rule mechanism.
func activateTopoCustomRules(qsc tabletserver.Controller) {
	if customrule.DatabaseCustomRuleEnable {
		qsc.RegisterQueryRuleSource(databaseCustomRuleSource)

		cr, err := newDatabaseCustomRule(qsc)
		if err != nil {
			log.Fatalf("cannot start DatabaseCustomRule: %v", err)
		}
		customrule.WaitForFilter = cr.WaitForFilter
		cr.start()

		servenv.OnTerm(cr.stop)
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, activateTopoCustomRules)
}

func (cr *databaseCustomRule) WaitForFilter(name string, shouldExists bool) error {
	timeoutDuration := 5 * time.Second
	interval := 100 * time.Millisecond

	timeout := time.After(timeoutDuration)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	err := cr.reloadRulesFromDatabase()
	if err != nil {
		return fmt.Errorf("failed to reload rules from database: %v", err)
	}

	for {
		select {
		case <-timeout:
			return fmt.Errorf("wait for filter reload timeout")
		case <-ticker.C:
			filter := cr.FindFilter(name)
			if shouldExists && filter != nil || !shouldExists && filter == nil {
				return nil
			}
			err := cr.reloadRulesFromDatabase()
			if err != nil {
				return fmt.Errorf("failed to reload rules from database: %v", err)
			}
		}
	}
}

func (cr *databaseCustomRule) FindFilter(name string) *rules.Rule {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr.qrs.Find(name)
}
