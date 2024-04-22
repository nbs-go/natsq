package natsq

import (
	"fmt"
	"github.com/nbs-go/nlogger/v2"
)

type InitOptionSetter func(o *initOption)

type initOption struct {
	h Handler
}

func WithHandler(h Handler) InitOptionSetter {
	if h == nil {
		panic(fmt.Errorf("natsq: cannot set empty Handler"))
	}
	return func(o *initOption) {
		o.h = h
	}
}

// evaluateInitOptions evaluates Init options and override default value when set
func evaluateInitOptions(log nlogger.Logger, args []InitOptionSetter) *initOption {
	o := initOption{
		h: &noHandler{log: log},
	}
	for _, fn := range args {
		fn(&o)
	}
	return &o
}
