package thanos_engine

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-community/promql-engine/engine"
	"github.com/thanos-community/promql-engine/logicalplan"

	"github.com/cortexproject/promqlsmith"
)

func TestPromQLCompatibility(t *testing.T) {
	load := `load 30s
http_requests_total{pod="nginx-1", series="1"} 1+1.1x40
http_requests_total{pod="nginx-2", series="2"} 2+2.3x50
http_requests_total{pod="nginx-3", series="3"} 6+0.8x60
http_requests_total{pod="nginx-4", series="3"} 5+2.4x50
http_requests_total{pod="nginx-5", series="1"} 8.4+2.3x50
http_requests_total{pod="nginx-6", series="2"} 2.3+2.3x50
`
	test, err := promql.NewTest(t, load)
	testutil.Ok(t, err)
	defer test.Close()

	testutil.Ok(t, test.Run())
	ctx := test.Context()
	series, err := getSeries(ctx, test.Queryable())
	testutil.Ok(t, err)

	opts := promql.EngineOpts{
		Timeout:              time.Minute,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		EnablePerStepStats:   true,
		MaxSamples:           5000000000,
	}
	oldEngine := promql.NewEngine(opts)
	newOpts := engine.Opts{
		EngineOpts:        opts,
		DisableFallback:   false,
		LogicalOptimizers: logicalplan.AllOptimizers,
	}
	newEngine := engine.New(newOpts)

	now := time.Now()
	rnd := rand.New(rand.NewSource(now.Unix()))
	ps := promqlsmith.New(rnd, series, true, true)

	for i := 0; i < 10; i++ {
		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)
		t.Logf("Running instant query %s\n", query)
		//level.Info(logger).Log("msg", "start running instant query", "query", query)
		q1, err := oldEngine.NewInstantQuery(test.Queryable(), nil, query, now)
		testutil.Ok(t, err)

		q2, err := newEngine.NewInstantQuery(test.Queryable(), nil, query, now)
		testutil.Ok(t, err)

		oldResult := q1.Exec(context.Background())
		testutil.Ok(t, oldResult.Err)
		newResult := q2.Exec(context.Background())
		testutil.Equals(t, true, newResult != nil)
		testutil.Ok(t, newResult.Err)

		if hasNaNs(oldResult) {
			t.Log("Applying comparison with NaN equality.")
			testutil.WithGoCmp(cmpopts.EquateNaNs()).Equals(t, oldResult, newResult)
		} else {
			emptyLabelsToNil(oldResult)
			emptyLabelsToNil(newResult)
			testutil.Equals(t, oldResult, newResult)
		}
		q1.Close()
		q2.Close()
	}
}

func getSeries(ctx context.Context, q storage.Queryable) ([]labels.Labels, error) {
	querier, err := q.Querier(ctx, 0, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	res := make([]labels.Labels, 0)
	ss := querier.Select(false, &storage.SelectHints{Func: "series"}, labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"))
	for ss.Next() {
		lbls := ss.At().Labels()
		res = append(res, lbls)
	}
	if err := ss.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func hasNaNs(result *promql.Result) bool {
	switch result := result.Value.(type) {
	case promql.Matrix:
		for _, vector := range result {
			for _, point := range vector.Points {
				if math.IsNaN(point.V) {
					return true
				}
			}
		}
	case promql.Vector:
		for _, point := range result {
			if math.IsNaN(point.V) {
				return true
			}
		}
	case promql.Scalar:
		return math.IsNaN(result.V)
	}

	return false
}

// emptyLabelsToNil sets empty labelsets to nil to work around inconsistent
// results from the old engine depending on the literal type (e.g. number vs. compare).
func emptyLabelsToNil(result *promql.Result) {
	if value, ok := result.Value.(promql.Matrix); ok {
		for i, s := range value {
			if len(s.Metric) == 0 {
				result.Value.(promql.Matrix)[i].Metric = nil
			}
		}
	}
}
