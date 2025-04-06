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

func TestPromQLSmith_Walk_RespectsExprAndValueTypeConstraints(t *testing.T) {
	tests := []struct {
		name           string
		supportedExprs []ExprType
		valueTypes     []parser.ValueType
		wantNil        bool
	}{
		{
			name:           "vector only expressions with vector value type",
			supportedExprs: []ExprType{VectorSelector, AggregateExpr},
			valueTypes:     []parser.ValueType{parser.ValueTypeVector},
			wantNil:        false,
		},
		{
			name:           "scalar only expressions with vector value type",
			supportedExprs: []ExprType{NumberLiteral},
			valueTypes:     []parser.ValueType{parser.ValueTypeVector},
			wantNil:        true, // Should return nil as intersection is empty
		},
		{
			name:           "mixed expressions with scalar value type",
			supportedExprs: []ExprType{VectorSelector, NumberLiteral, BinaryExpr},
			valueTypes:     []parser.ValueType{parser.ValueTypeScalar},
			wantNil:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := New(
				rand.New(rand.NewSource(1)), // Fixed seed for reproducibility
				[]labels.Labels{labels.FromStrings("foo", "bar")},
				WithEnabledExprs(tt.supportedExprs),
			)

			expr := ps.Walk(tt.valueTypes...)

			if tt.wantNil {
				if expr != nil {
					t.Errorf("Walk() = %v, want nil", expr)
				}
				return
			}

			if expr == nil {
				t.Fatal("Walk() returned nil, want non-nil expression")
			}

			// Verify the expression type matches one of the supported types
			found := false
			for _, supportedType := range tt.supportedExprs {
				if exprMatchesType(expr, supportedType) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Walk() returned expression of unexpected type: %T", expr)
			}
		})
	}
}

// Helper function to match expression types
func exprMatchesType(expr parser.Expr, exprType ExprType) bool {
	_, ok := expr.(*parser.ParenExpr)
	if ok {
		return true
	}

	switch exprType {
	case VectorSelector:
		_, ok := expr.(*parser.VectorSelector)
		return ok
	case NumberLiteral:
		_, ok := expr.(*parser.NumberLiteral)
		return ok
	case BinaryExpr:
		_, ok := expr.(*parser.BinaryExpr)
		return ok
	case AggregateExpr:
		_, ok := expr.(*parser.AggregateExpr)
		return ok
	case CallExpr:
		_, ok := expr.(*parser.Call)
		return ok
	case UnaryExpr:
		_, ok := expr.(*parser.UnaryExpr)
		return ok
	case SubQueryExpr:
		_, ok := expr.(*parser.SubqueryExpr)
		return ok
	case MatrixSelector:
		_, ok := expr.(*parser.MatrixSelector)
		return ok
	default:
		return false
	}
}

func TestPromQLSmith_Walk_RespectsMaxDepth(t *testing.T) {
	maxDepth := 5
	ps := New(
		rand.New(rand.NewSource(1)), // Fixed seed for reproducibility
		testSeriesSet,
		WithMaxDepth(maxDepth),
	)

	// Generate multiple expressions to increase confidence
	for i := 0; i < 1000; i++ {
		expr := ps.Walk()
		fmt.Println(expr.Pretty(0))
		depth := getExprDepth(expr)
		require.LessOrEqual(t, depth, maxDepth, "expression depth %d exceeds maximum depth %d for expression: %s", depth, maxDepth, expr.String())
	}
}

// getExprDepth returns the maximum depth of an expression tree
func getExprDepth(expr parser.Expr) int {
	if expr == nil {
		return 0
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return 1 + max(getExprDepth(e.LHS), getExprDepth(e.RHS))
	case *parser.UnaryExpr:
		return 1 + getExprDepth(e.Expr)
	case *parser.ParenExpr:
		return getExprDepth(e.Expr)
	case *parser.AggregateExpr:
		return 1 + getExprDepth(e.Expr)
	case *parser.Call:
		maxArgDepth := 0
		for _, arg := range e.Args {
			argDepth := getExprDepth(arg)
			maxArgDepth = max(maxArgDepth, argDepth)
		}
		return 1 + maxArgDepth
	case *parser.SubqueryExpr:
		return 1 + getExprDepth(e.Expr)
	case *parser.MatrixSelector:
		return getExprDepth(e.VectorSelector)
	default:
		return 1
	}
}
