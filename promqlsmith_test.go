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
			"cluster":         "us-west-2",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "404",
			"cluster":         "us-west-2",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "500",
			"cluster":         "us-west-2",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "prometheus",
			"cluster":         "us-west-2",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "node_exporter",
			"cluster":         "us-west-2",
			"env":             "prod",
		}),

		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "200",
			"cluster":         "us-east-1",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "404",
			"cluster":         "us-east-1",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "500",
			"cluster":         "us-east-1",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "prometheus",
			"cluster":         "us-east-1",
			"env":             "prod",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "up",
			"job":             "node_exporter",
			"cluster":         "us-east-1",
			"env":             "prod",
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
	_, err := engine.NewInstantQuery(ctx, q, &promql.PrometheusQueryOpts{}, result, time.Now())
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
	_, err := engine.NewRangeQuery(ctx, q, &promql.PrometheusQueryOpts{}, result, time.Now().Add(-time.Hour), time.Now(), time.Minute)
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

func TestWalkSelectors(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ps := New(rnd, testSeriesSet)
	matchers := ps.WalkSelectors()
	minLen := (len(ps.labelNames) + 1) / 2
	require.True(t, len(matchers) >= minLen)

	enforcedMatcher := labels.MustNewMatcher(labels.MatchEqual, "test", "aaa")
	opts := []Option{WithEnforceLabelMatchers([]*labels.Matcher{enforcedMatcher})}
	psWithEnforceMatchers := New(rnd, testSeriesSet, opts...)
	matchers = psWithEnforceMatchers.WalkSelectors()
	minLen = (len(ps.labelNames) + 1) / 2
	require.True(t, len(matchers) >= minLen)
	var found bool
	for _, matcher := range matchers {
		if matcher == enforcedMatcher {
			found = true
		}
	}
	require.True(t, found)
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
