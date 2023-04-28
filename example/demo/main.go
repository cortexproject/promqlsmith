package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"

	"github.com/cortexproject/promqlsmith"
)

var (
	unsupportedFunctions = map[string]struct{}{
		"histogram_count":    {},
		"histogram_sum":      {},
		"histogram_fraction": {},
		"present_over_time":  {},
		"acos":               {},
		"acosh":              {},
		"asin":               {},
		"asinh":              {},
		"atan":               {},
		"atanh":              {},
		"cos":                {},
		"cosh":               {},
		"sin":                {},
		"sinh":               {},
		"tan":                {},
		"tanh":               {},
		"dag":                {},
		"pi":                 {},
		"rad":                {},
	}

	enabledBinops = []parser.ItemType{
		parser.SUB,
		parser.ADD,
		parser.MUL,
		parser.MOD,
		parser.DIV,
		parser.EQLC,
		parser.NEQ,
		parser.LTE,
		parser.GTE,
		parser.LSS,
		parser.GTR,
		parser.POW,
		parser.LAND,
		parser.LOR,
		parser.LUNLESS,
	}
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	if err := run(); err != nil {
		level.Error(logger).Log("msg", "failed to run", "err", err)
		os.Exit(1)
	}
}

func run() error {
	client, err := api.NewClient(api.Config{
		Address: "https://prometheus.demo.do.prometheus.io",
	})
	if err != nil {
		return errors.Wrapf(err, "create Prometheus client")
	}
	promAPI := v1.NewAPI(client)
	ctx := context.Background()
	now := time.Now()
	series, _, err := promAPI.Series(
		ctx,
		[]string{"{job=\"prometheus\"}"},
		now.Add(-2*time.Hour), now,
	)
	if err != nil {
		return errors.Wrapf(err, "get series")
	}
	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(getAvailableFunctions()),
		promqlsmith.WithEnabledBinOps(enabledBinops),
		promqlsmith.WithEnableVectorMatching(true),
	}
	ps := promqlsmith.New(rnd, modelLabelSetToLabels(series), opts...)
	expr := ps.WalkInstantQuery()
	query := expr.Pretty(0)
	fmt.Printf("Running instant query:\n%s\n", query)

	res, _, err := promAPI.Query(ctx, query, now)
	if err != nil {
		return errors.Wrapf(err, "instant query")
	}

	fmt.Println(res)
	return nil
}

func modelLabelSetToLabels(labelSets []model.LabelSet) []labels.Labels {
	out := make([]labels.Labels, len(labelSets))
	bufLabels := labels.EmptyLabels()
	builder := labels.NewBuilder(bufLabels)
	for i, lbls := range labelSets {
		for k, v := range lbls {
			builder.Set(string(k), string(v))
		}
		out[i] = builder.Labels()
		builder.Reset(bufLabels)
	}
	return out
}

// Demo Prometheus is still at v2.27, some functions are not supported.
func getAvailableFunctions() []*parser.Function {
	res := make([]*parser.Function, 0)
	for _, f := range parser.Functions {
		if f.Variadic != 0 {
			continue
		}
		if slices.Contains(f.ArgTypes, parser.ValueTypeString) {
			continue
		}
		if _, ok := unsupportedFunctions[f.Name]; ok {
			continue
		}
		res = append(res, f)
	}
	return res
}
