package binlogconsumer

import (
	"context"
	"github.com/spf13/pflag"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	enableCdcConsumer         = true
	cdcConsumerReloadInterval = 10 * time.Second
)

var cdcConsumerController *CdcConsumerController

func init() {
	servenv.OnParseFor("vtgate", registerCdcConsumerFlags)

	servenv.OnRun(func() {
		if !enableCdcConsumer {
			log.Info("CDC Consumer is disabled")
			return
		}
		cdcConsumerController = NewCdcConsumerController()
		cdcConsumerController.start()
	})

	servenv.OnClose(func() {
		if !enableCdcConsumer {
			return
		}
		cdcConsumerController.stop()
	})
}

func registerCdcConsumerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&enableCdcConsumer, "enable_cdc_consumer", enableCdcConsumer, "enable CDC Consumer")
	fs.DurationVar(&cdcConsumerReloadInterval, "cdc_consumer_reload_interval", cdcConsumerReloadInterval, "reload interval for CDC Consumer")
}

type CdcConsumerController struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex

	consumer map[string]*CdcConsumer
}

type CdcConsumer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	wasiRuntimeContext *WasiRuntimeContext
}

func NewCdcConsumerController() *CdcConsumerController {
	w := &CdcConsumerController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.consumer = make(map[string]*CdcConsumer)
	return w
}

func (cr *CdcConsumerController) start() {
	go func() {
		intervalTicker := time.NewTicker(cdcConsumerReloadInterval)
		defer intervalTicker.Stop()

		for {
			select {
			case <-cr.ctx.Done():
				log.Infof("Background watch of cdc consumer was terminated")
				return
			case <-intervalTicker.C:
			}

			if err := cr.reloadCdcConsumer(); err != nil {
				log.Warningf("Background watch of cdc consumer failed: %v", err)
			}
		}
	}()
}

func (cr *CdcConsumerController) stop() {
	cr.cancelFunc()
}

// todo: implement this methods, currently it is just a stub
func (cr *CdcConsumerController) reloadCdcConsumer() error {
	cr.mu.Lock()

	context.WithCancel(cr.ctx)
	consumer := &CdcConsumer{}
	consumer.ctx, consumer.cancelFunc = context.WithCancel(cr.ctx)
	consumer.wasiRuntimeContext = NewWasiRuntimeContext()
	cr.consumer["test"] = consumer
	defer consumer.wasiRuntimeContext.run()

	cr.mu.Unlock()

	return nil
}
