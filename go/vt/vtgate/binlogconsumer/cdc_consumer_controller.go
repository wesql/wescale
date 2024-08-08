package binlogconsumer

import (
	"context"
	"github.com/golang/glog"
	stdLog "log"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/internal/global"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var (
	EnableCdcConsumer         = true
	cdcConsumerReloadInterval = 10 * time.Second
)

func init() {
	servenv.OnParseFor("vtgate", RegisterCdcConsumerFlags)
}

func RegisterCdcConsumerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&EnableCdcConsumer, "enable_cdc_consumer", EnableCdcConsumer, "enable CDC Consumer")
	fs.DurationVar(&cdcConsumerReloadInterval, "cdc_consumer_reload_interval", cdcConsumerReloadInterval, "reload interval for CDC Consumer")
}

type CdcConsumerController struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex

	consumer map[string]*CdcConsumer
	svc      queryservice.QueryService
}

type CdcConsumer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex

	svc queryservice.QueryService

	id             int64
	name           string
	enable         bool
	wasmBinaryName string
	tag            string
	env            string

	wasiRuntimeContext *WasiRuntimeContext
	err                error

	logger *stdLog.Logger
}

func NewCdcConsumerController(svc queryservice.QueryService) *CdcConsumerController {
	w := &CdcConsumerController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.consumer = make(map[string]*CdcConsumer)
	w.svc = svc
	return w
}

func (cr *CdcConsumerController) Start() {
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

func (cr *CdcConsumerController) Stop() {
	cr.cancelFunc()
}

func (cr *CdcConsumerController) reloadCdcConsumer() error {
	cr.mu.Lock()

	target := &querypb.Target{
		Keyspace:   global.DefaultKeyspace,
		Shard:      global.DefaultShard,
		TabletType: topodata.TabletType_PRIMARY,
	}
	qr, err := cr.svc.ExecuteInternal(cr.ctx, target, "select * from mysql.cdc_consumer", nil, 0, 0, nil)
	if err != nil {
		return err
	}
	for _, row := range qr.Named().Rows {

		id := row.AsInt64("id", 0)
		name := row.AsString("name", "")
		enable := row.AsBool("enable", false)
		wasmBinaryName := row.AsString("wasm_binary_name", "")
		tag := row.AsString("tag", "")
		env := row.AsString("env", "")

		if !enable {
			continue
		}

		consumer := &CdcConsumer{
			svc:            cr.svc,
			id:             id,
			name:           name,
			enable:         enable,
			wasmBinaryName: wasmBinaryName,
			tag:            tag,
			env:            env,
			logger:         glog.NewStandardLogger("INFO"),
		}
		consumer.ctx, consumer.cancelFunc = context.WithCancel(cr.ctx)
		cr.consumer[name] = consumer

		go consumer.loadAndRunWasm()
	}

	cr.mu.Unlock()
	return nil
}

func (cc *CdcConsumer) loadAndRunWasm() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.wasiRuntimeContext != nil || cc.err != nil {
		// already running or completed/failed
		return
	}

	bytes, err := GetWasmBytesByBinaryName(cc.ctx, cc.wasmBinaryName, cc.svc)
	if err != nil {
		cc.err = err
		log.Errorf("Failed to get cdc consumer wasm: %v", err)
		return
	}

	wrc := NewWasiRuntimeContext(cc.wasmBinaryName, strings.Split(cc.env, " "), bytes, cc.logger)
	cc.wasiRuntimeContext = wrc
	err = wrc.run(cc.ctx)
	if err != nil {
		cc.err = err
		log.Errorf("Failed to run cdc consumer wasm: %v", err)
	}
}
