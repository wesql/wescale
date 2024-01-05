package viperutil

import (
	"sync"

	"github.com/spf13/pflag"
)

type Reloader struct {
	mu         sync.Mutex
	handlerMap map[string]func(key string, value string, fs *pflag.FlagSet)
}

func NewConfigReloader() *Reloader {
	return &Reloader{
		handlerMap: make(map[string]func(key string, value string, fs *pflag.FlagSet)),
	}
}

func (r *Reloader) AddReloadHandler(key string, handler func(key string, value string, fs *pflag.FlagSet)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlerMap[key] = handler
}

func (r *Reloader) handleConfigChange(key string, value string, fs *pflag.FlagSet) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if h, ok := r.handlerMap[key]; ok {
		h(key, value, fs)
	}
}
