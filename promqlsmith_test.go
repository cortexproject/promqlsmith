package promqlsmith

import (
	"context"
	"fmt"
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
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	ps := New(rnd, testSeriesSet, opts...)
	expr := ps.WalkInstantQuery()
	result := expr.Pretty(0)
	engine := promql.NewEngine(promql.EngineOpts{
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})
	q := &storage.MockQueryable{}
	ctx := context.Background()
	_, err := engine.NewInstantQuery(ctx, q, &promql.QueryOpts{}, result, time.Now())
	require.NoError(t, err)
}

func TestWalkRangeQuery(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	ps := New(rnd, testSeriesSet, opts...)
	expr := ps.WalkRangeQuery()
	result := expr.Pretty(0)
	engine := promql.NewEngine(promql.EngineOpts{
		EnableNegativeOffset: true,
		EnableAtModifier:     true,
	})
	q := &storage.MockQueryable{}
	ctx := context.Background()
	_, err := engine.NewRangeQuery(ctx, q, &promql.QueryOpts{}, result, time.Now().Add(-time.Hour), time.Now(), time.Minute)
	require.NoError(t, err)
}

func TestWalk(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	ps := New(rnd, testSeriesSet, opts...)
	expr := ps.Walk()
	result := expr.Pretty(0)
	_, err := parser.ParseExpr(result)
	require.NoError(t, err)
}

func TestFilterEmptySeries(t *testing.T) {
	for i, tc := range []struct {
		ss       []labels.Labels
		expected []labels.Labels
	}{
		{
			ss:       nil,
			expected: []labels.Labels{},
		},
		{
			ss:       []labels.Labels{labels.EmptyLabels()},
			expected: []labels.Labels{},
		},
		{
			ss:       []labels.Labels{labels.FromStrings("foo", "bar")},
			expected: []labels.Labels{labels.FromStrings("foo", "bar")},
		},
		{
			ss:       testSeriesSet,
			expected: testSeriesSet,
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			output := filterEmptySeries(tc.ss)
			require.Equal(t, tc.expected, output)
		})
	}
}
