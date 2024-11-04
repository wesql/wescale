/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package tabletserver

import (
	"context"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const SeleteWorkflowNameFromBranchJobs = "SELECT * from mysql.branch_jobs;"

const SeleteVReplicationByWorkflow = "SELECT * from mysql.vreplication where workflow=%a;"

const UpdateBranchJobStatusByWorkflow = "update mysql.branch_jobs set status=%a,message=%a where workflow_name=%a"

const (
	BranchStateOfPrepare   = "Prepare"
	BranchStateOfRunning   = "Running"
	BranchStateOfStop      = "Stop"
	BranchStateOfCompleted = "Completed"
	BranchStateOfError     = "Error"
)

const UpdateInterval = 2 * time.Second

type BranchWatcher struct {
	dbConfig dbconfigs.Connector
	conns    *connpool.Pool

	ticker         *time.Ticker
	updateInterval time.Duration
	running        bool
}

func NewBranchWatcher(env tabletenv.Env, dbConfig dbconfigs.Connector) *BranchWatcher {
	branchWatcher := BranchWatcher{
		running:        false,
		ticker:         time.NewTicker(UpdateInterval),
		updateInterval: UpdateInterval,
		dbConfig:       dbConfig,
	}
	branchWatcher.conns = connpool.NewPool(env, "BranchWatch", tabletenv.ConnPoolConfig{
		Size:               2,
		IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
	})
	return &branchWatcher
}

func (b *BranchWatcher) updateBranchState(ctx context.Context, conn *connpool.DBConn, state string, message, workflow string) error {
	query, err := sqlparser.ParseAndBind(UpdateBranchJobStatusByWorkflow,
		sqltypes.StringBindVariable(state),
		sqltypes.StringBindVariable(message),
		sqltypes.StringBindVariable(workflow))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, query, -1, false)
	if err != nil {
		return err
	}
	return nil
}

func (b *BranchWatcher) watch() error {
	ctx := context.Background()
	conn, err := b.conns.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	qr, err := conn.Exec(ctx, SeleteWorkflowNameFromBranchJobs, -1, true)
	if err != nil {
		return err
	}
	for _, row := range qr.Named().Rows {
		workflow := row["workflow_name"].ToString()
		query, err := sqlparser.ParseAndBind(SeleteVReplicationByWorkflow, sqltypes.StringBindVariable(workflow))
		if err != nil {
			return err
		}
		vreplication, err := conn.Exec(ctx, query, -1, true)
		rows := vreplication.Named().Row()
		vState := rows["state"].ToString()
		message := rows["message"].ToString()
		if err != nil {
			return err
		}
		switch vState {
		case binlogplayer.BlpStopped:
			if strings.Contains(message, "Stopped after copy") {
				err = b.updateBranchState(ctx, conn, BranchStateOfCompleted, message, workflow)
				if err != nil {
					return err
				}
			} else {
				err = b.updateBranchState(ctx, conn, BranchStateOfStop, message, workflow)
				if err != nil {
					return err
				}
			}
		case binlogplayer.BlpError:
			err = b.updateBranchState(ctx, conn, BranchStateOfError, message, workflow)
			if err != nil {
				return err
			}
		case binlogplayer.VReplicationCopying, binlogplayer.BlpRunning:
			err = b.updateBranchState(ctx, conn, BranchStateOfRunning, message, workflow)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *BranchWatcher) Open() {
	log.Tracef("BranchWatcher Open")
	b.conns.Open(b.dbConfig, b.dbConfig, b.dbConfig)
	b.ticker.Reset(b.updateInterval)
	if !b.running {
		go func() {
			log.Tracef("BranchWatcher started")
			for range b.ticker.C {
				err := b.watch()
				if err != nil {
					log.Errorf("BranchWatcher error: %v", err)
				}
			}
		}()
		b.running = true
	}
}
func (b *BranchWatcher) Close() {
	b.conns.Close()
	b.ticker.Stop()
	b.running = false
}
