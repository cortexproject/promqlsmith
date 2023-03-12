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

	"github.com/cortexproject/promqlsmith"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	if err := run(logger); err != nil {
		level.Error(logger).Log("msg", "failed to run", "err", err)
		os.Exit(1)
	}
}

func run(logger log.Logger) error {
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
	}
	ps := promqlsmith.New(rnd, modelLabelSetToLabels(series), opts...)
	expr := ps.WalkInstantQuery()
	query := expr.Pretty(0)
	level.Info(logger).Log("msg", "running instant query", "query", query)

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
		out[i] = builder.Labels(bufLabels)
		builder.Reset(bufLabels)
	}
	return out
}
