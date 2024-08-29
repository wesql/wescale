package binlogconsumer

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/stealthrocket/wasi-go"
	"github.com/stealthrocket/wasi-go/imports"
	"github.com/stealthrocket/wasi-go/imports/wasi_http"
	"github.com/tetratelabs/wazero"
	"net"
	"net/http"
)

// WasiRuntimeContext is a struct that holds the configuration for the WASI runtime.
// It represents the lifecycle of a WASM Program execution.
type WasiRuntimeContext struct {
	wasmName    string
	wasmProgram []byte
	wasmArgs    []string

	envs             stringList
	dirs             stringList
	listens          stringList
	dials            stringList
	dnsServer        string
	socketExt        string
	pprofAddr        string
	wasiHttp         string
	wasiHttpAddr     string
	wasiHttpPath     string
	trace            bool
	tracerStringSize int
	nonBlockingStdio bool
	maxOpenFiles     int
	maxOpenDirs      int
}

func NewWasiRuntimeContext(wasmBinaryName string, envList []string, wasmBytes []byte) *WasiRuntimeContext {
	return &WasiRuntimeContext{
		wasmName:    wasmBinaryName,
		wasmProgram: wasmBytes,
		wasmArgs:    []string{},
		envs:        envList,
		// todo cdc: we need to provide minimal set of directories for CDC WASM Program
		dirs:             stringList{"/"},
		listens:          stringList{},
		dials:            stringList{},
		dnsServer:        "",
		socketExt:        "auto",
		pprofAddr:        "",
		wasiHttp:         "auto",
		wasiHttpAddr:     "",
		wasiHttpPath:     "/",
		trace:            false,
		tracerStringSize: 32,
		nonBlockingStdio: false,
		maxOpenFiles:     1024,
		maxOpenDirs:      1024,
	}
}

func (config *WasiRuntimeContext) run(ctx context.Context) error {
	if config.dnsServer != "" {
		_, dnsServerPort, _ := net.SplitHostPort(config.dnsServer)
		net.DefaultResolver.PreferGo = true
		net.DefaultResolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			var d net.Dialer
			if dnsServerPort != "" {
				address = config.dnsServer
			} else {
				_, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, net.InvalidAddrError(address)
				}
				address = net.JoinHostPort(config.dnsServer, port)
			}
			return d.DialContext(ctx, network, address)
		}
	}
	if config.pprofAddr != "" {
		go http.ListenAndServe(config.pprofAddr, nil)
	}

	runtime := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
	defer runtime.Close(ctx)

	wasmModule, err := runtime.CompileModule(ctx, config.wasmProgram)
	if err != nil {
		return err
	}
	defer wasmModule.Close(ctx)

	builder := imports.NewBuilder().
		WithName(config.wasmName).
		WithArgs(config.wasmArgs...).
		WithEnv(config.envs...).
		WithDirs(config.dirs...).
		WithListens(config.listens...).
		WithDials(config.dials...).
		WithNonBlockingStdio(config.nonBlockingStdio).
		WithSocketsExtension(config.socketExt, wasmModule).
		//WithTracer(config.trace, os.Stderr, wasi.WithTracerStringSize(config.tracerStringSize)).
		WithMaxOpenFiles(config.maxOpenFiles).
		WithMaxOpenDirs(config.maxOpenDirs)

	var system wasi.System
	ctx, system, err = builder.Instantiate(ctx, runtime)
	if err != nil {
		return err
	}
	defer system.Close(ctx)

	importWasi := false
	var wasiHTTP *wasi_http.WasiHTTP = nil
	switch config.wasiHttp {
	case "auto":
		importWasi = wasi_http.DetectWasiHttp(wasmModule)
	case "v1":
		importWasi = true
	case "none":
		importWasi = false
	default:
		return fmt.Errorf("invalid value for -http '%v', expected 'auto', 'v1' or 'none'", config.wasiHttp)
	}
	if importWasi {
		wasiHTTP = wasi_http.MakeWasiHTTP()
		if err := wasiHTTP.Instantiate(ctx, runtime); err != nil {
			return err
		}
	}

	moduleConfig := wazero.NewModuleConfig()
	instance, err := runtime.InstantiateModule(ctx, wasmModule, moduleConfig)
	if err != nil {
		return err
	}
	if len(config.wasiHttpAddr) > 0 {
		handler := wasiHTTP.MakeHandler(instance)
		http.Handle(config.wasiHttpPath, handler)
		return http.ListenAndServe(config.wasiHttpAddr, nil)
	}
	return instance.Close(ctx)
}

type stringList []string

func (s stringList) String() string {
	return fmt.Sprintf("%v", []string(s))
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}
