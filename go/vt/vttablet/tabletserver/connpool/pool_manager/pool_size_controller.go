package pool_manager

import (
	"context"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/background"
)

const reservedConnectionCount = 35 + 20

var poolSizeAutoAjustmentInterval = 30 * time.Second

type PoolSizeController struct {
	isOpen     atomic.Bool
	ctx        context.Context
	cancelFunc context.CancelFunc
	taskPool   *background.TaskPool
}

// 可以从TaskEngine获取MySQL连接。用完需要调用: defer conn.Recycle()
// func (te *TaskPool) BorrowConn(ctx context.Context, setting *pools.Setting) (*connpool.DBConn, error)

func NewPoolSizeController(taskPool *background.TaskPool) *PoolSizeController {
	return &PoolSizeController{
		taskPool: taskPool,
	}
}

func (psc *PoolSizeController) Open() {
	if psc.isOpen.Load() {
		return
	}
	log.Info("PoolSizeController opened")
	psc.ctx, psc.cancelFunc = context.WithCancel(context.Background())
	go psc.Start()
	psc.isOpen.Store(true)
}

func (psc *PoolSizeController) Close() {
	if !psc.isOpen.Load() {
		return
	}
	log.Info("PoolSizeController closed")
	psc.cancelFunc()
	psc.isOpen.Store(false)
}

func (psc *PoolSizeController) Start() {
	t := time.NewTicker(poolSizeAutoAjustmentInterval)
	defer t.Stop()
	for {
		select {
		case <-psc.ctx.Done():
			return
		case <-t.C:
		}
		psc.Reconcile()
	}
}

func (psc *PoolSizeController) Reconcile() {
	conn, err := psc.taskPool.BorrowConn(psc.ctx, nil)
	if err != nil {
		log.Errorf("fail to acquire connection from task pool, err: %v", err)
		return
	}
	defer conn.Recycle()

	// 我需要根据MySQL的max_connections，以及其他指标，比如Threads_connected，Threads_running，Max_used_connections等（你在考虑一下其他指标）
	// 设置我的2个连接池数量：txPoolSize, oltpReadPoolSize。
	// 所有的DML，DDL或者有状态的连接都会使用txPoolSize
	// 所有的无状态连接都使用oltpReadPoolSize
	// 你可以假定，可以通过IResourcePool的接口获取到这2个连接池的所有信息。

}
