package binlogconsumer

import (
	"context"
	"github.com/spf13/pflag"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate"
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
		cdcConsumerController = NewCdcConsumerController(vtgate.GetExecutor())
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
	executor *vtgate.Executor
}

type CdcConsumer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	id               int64
	name             string
	enable           bool
	wasm_binary_name string
	cdcType          string
	env              string

	wasiRuntimeContext *WasiRuntimeContext
}

func NewCdcConsumerController(e *vtgate.Executor) *CdcConsumerController {
	w := &CdcConsumerController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.consumer = make(map[string]*CdcConsumer)
	w.executor = e
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

	qr, err := cr.executor.QueryCdcConsumer()
	if err != nil {
		return err
	}
	for _, row := range qr.Named().Rows {

		id := row.AsInt64("id", 0)
		name := row.AsString("name", "")
		enable := row.AsBool("enable", false)
		wasm_binary_name := row.AsString("wasm_binary_name", "")
		cdcType := row.AsString("type", "")
		env := row.AsString("env", "")

		if !enable || cdcType != "cdc_consumer" {
			continue
		}

		consumer := &CdcConsumer{
			id:               id,
			name:             name,
			enable:           enable,
			wasm_binary_name: wasm_binary_name,
			cdcType:          cdcType,
			env:              env,
		}
		consumer.ctx, consumer.cancelFunc = context.WithCancel(cr.ctx)
		//consumer.wasiRuntimeContext = NewWasiRuntimeContext()

		cr.consumer[name] = consumer
	}

	//defer consumer.wasiRuntimeContext.run()
	cr.mu.Unlock()
	return nil
}
