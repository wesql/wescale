package background

import (
	"context"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var backGroundTaskPoolConfig = tabletenv.ConnPoolConfig{
	Size:               1,
	MaxSize:            50,
	TimeoutSeconds:     1,
	IdleTimeoutSeconds: 30 * 60,
	MaxWaiters:         500,
}

func registerbackGroundTaskPoolConfigFlags(fs *pflag.FlagSet) {
	fs.IntVar(&backGroundTaskPoolConfig.Size, "background_task_pool_size", backGroundTaskPoolConfig.Size, "task background task connection pool size")
	fs.IntVar(&backGroundTaskPoolConfig.MaxSize, "background_task_pool_max_size", backGroundTaskPoolConfig.MaxSize, "task background task connection pool max size")
	tabletenv.SecondsVar(fs, &backGroundTaskPoolConfig.TimeoutSeconds, "background_task_pool_timeout", backGroundTaskPoolConfig.TimeoutSeconds, "task background task connection pool timeout")
	tabletenv.SecondsVar(fs, &backGroundTaskPoolConfig.IdleTimeoutSeconds, "background_task_pool_idle_timeout", backGroundTaskPoolConfig.IdleTimeoutSeconds, "task background task connection pool idle timeout")
	fs.IntVar(&backGroundTaskPoolConfig.MaxWaiters, "background_task_pool_max_waiters", backGroundTaskPoolConfig.MaxWaiters, "task background task connection pool max waiters")
}

func init() {
	servenv.OnParseFor("vttablet", registerbackGroundTaskPoolConfigFlags)
}

type TaskPool struct {
	isOpen bool
	env    tabletenv.Env

	// Pools
	conns *connpool.Pool
}

//todo use stats to monitor the pool usage

func NewTaskPool(env tabletenv.Env) *TaskPool {
	te := &TaskPool{
		env:   env,
		conns: connpool.NewPool(env, "TaskPool", backGroundTaskPoolConfig),
	}
	return te
}

func (te *TaskPool) BorrowConn(ctx context.Context, setting *pools.Setting) (*connpool.DBConn, error) {
	return te.conns.Get(ctx, setting)
}

func (te *TaskPool) Open() {
	if te.isOpen {
		return
	}
	log.Info("TaskPool: opening")
	te.conns.Open(te.env.Config().DB.AppWithDB(), te.env.Config().DB.DbaWithDB(), te.env.Config().DB.AppDebugWithDB())
	log.Info("TaskPool: opened")
	te.isOpen = true
}

func (te *TaskPool) Close() {
	if !te.isOpen {
		return
	}
	log.Info("TaskPool: closing")
	te.conns.Close()
	log.Info("TaskPool: closed")
	te.isOpen = false
}

func (te *TaskPool) InUse() int64 {
	if te.conns == nil {
		return 0
	}
	return te.conns.InUse()
}

func (te *TaskPool) SetCapacity(size int) {
	if te.conns == nil || size < 0 {
		return
	}
	te.conns.SetCapacity(size)
}
