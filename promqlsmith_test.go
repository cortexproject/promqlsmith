package promqlsmith

import (
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

var (
	testSeriesSet = []labels.Labels{
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "200",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "404",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "500",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "prometheus",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "node_exporter",
		}),
	}
)

func TestWalkInstantQuery(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ps := New(rnd, testSeriesSet, true, true)
	expr := ps.WalkInstantQuery()
	result := expr.Pretty(0)
	engine := promql.NewEngine(promql.EngineOpts{
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})
	q := &storage.MockQueryable{}
	_, err := engine.NewInstantQuery(q, &promql.QueryOpts{}, result, time.Now())
	require.NoError(t, err)
}

func TestWalkRangeQuery(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ps := New(rnd, testSeriesSet, true, true)
	expr := ps.WalkRangeQuery()
	result := expr.Pretty(0)
	engine := promql.NewEngine(promql.EngineOpts{
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})
	q := &storage.MockQueryable{}
	_, err := engine.NewRangeQuery(q, &promql.QueryOpts{}, result, time.Now().Add(-time.Hour), time.Now(), time.Minute)
	require.NoError(t, err)
}

func TestWalk(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ps := New(rnd, testSeriesSet, true, true)
	expr := ps.Walk()
	result := expr.Pretty(0)
	_, err := parser.ParseExpr(result)
	require.NoError(t, err)
}
