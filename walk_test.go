package promqlsmith

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestExprsFromReturnTypes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		valueTypes []parser.ValueType
		exprTypes  []ExprType
	}{
		{
			name:       "vector",
			valueTypes: []parser.ValueType{parser.ValueTypeVector},
			exprTypes:  []ExprType{VectorSelector, AggregateExpr, BinaryExpr, CallExpr, UnaryExpr},
		},
		{
			name:       "scalar",
			valueTypes: []parser.ValueType{parser.ValueTypeScalar},
			exprTypes:  []ExprType{BinaryExpr, CallExpr, NumberLiteral, UnaryExpr},
		},
		{
			name:       "matrix",
			valueTypes: []parser.ValueType{parser.ValueTypeMatrix},
			exprTypes:  []ExprType{MatrixSelector, SubQueryExpr},
		},
		{
			name:       "vector + scalar",
			valueTypes: []parser.ValueType{parser.ValueTypeVector, parser.ValueTypeScalar},
			exprTypes:  []ExprType{VectorSelector, AggregateExpr, BinaryExpr, CallExpr, NumberLiteral, UnaryExpr},
		},
		{
			name:       "vector + scalar + matrix",
			valueTypes: []parser.ValueType{parser.ValueTypeVector, parser.ValueTypeScalar, parser.ValueTypeMatrix},
			exprTypes:  []ExprType{VectorSelector, MatrixSelector, AggregateExpr, BinaryExpr, SubQueryExpr, CallExpr, NumberLiteral, UnaryExpr},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exprs := exprsFromValueTypes(tc.valueTypes)
			sort.Slice(exprs, func(i, j int) bool {
				return exprs[i] < exprs[j]
			})
			require.Equal(t, tc.exprTypes, exprs)
		})
	}
}

func TestWalkCall(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	for i, tc := range []struct {
		valueTypes []parser.ValueType
	}{
		{
			valueTypes: []parser.ValueType{},
		},
		{
			valueTypes: []parser.ValueType{parser.ValueTypeVector},
		},
		{
			valueTypes: []parser.ValueType{parser.ValueTypeScalar},
		},
		{
			valueTypes: vectorAndScalarValueTypes,
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			expr := p.walkCall(tc.valueTypes...)
			c, ok := expr.(*parser.Call)
			require.True(t, ok)
			if len(tc.valueTypes) == 0 {
				tc.valueTypes = allValueTypes
			}
			require.True(t, slices.Contains(tc.valueTypes, c.Func.ReturnType))
			for i, arg := range c.Args {
				require.Equal(t, c.Func.ArgTypes[i], arg.Type())
			}
		})
	}
}

func TestWalkBinaryExpr(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkBinaryExpr(parser.ValueTypeVector, parser.ValueTypeScalar)
	result := expr.Pretty(0)
	_, err := parser.ParseExpr(result)
	require.NoError(t, err)
}

func TestWalkAggregateParam(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	for i, tc := range []struct {
		op           parser.ItemType
		expectedFunc func(expr parser.Expr)
	}{
		{
			op: parser.TOPK,
			expectedFunc: func(expr parser.Expr) {
				require.Equal(t, parser.ValueTypeScalar, expr.Type())
			},
		},
		{
			op: parser.BOTTOMK,
			expectedFunc: func(expr parser.Expr) {
				require.Equal(t, parser.ValueTypeScalar, expr.Type())
			},
		},
		{
			op: parser.QUANTILE,
			expectedFunc: func(expr parser.Expr) {
				require.Equal(t, parser.ValueTypeScalar, expr.Type())
			},
		},
		{
			op: parser.COUNT_VALUES,
			expectedFunc: func(expr parser.Expr) {
				e, ok := expr.(*parser.StringLiteral)
				require.True(t, ok)
				require.Equal(t, e.Val, "value")
			},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			expr := p.walkAggregateParam(tc.op)
			tc.expectedFunc(expr)
		})
	}
}

func TestWrapParenExpr(t *testing.T) {
	for i, tc := range []struct {
		expr     parser.Expr
		expected parser.Expr
	}{
		{
			expr:     &parser.VectorSelector{},
			expected: &parser.VectorSelector{},
		},
		{
			expr:     &parser.AggregateExpr{},
			expected: &parser.AggregateExpr{},
		},
		{
			expr: &parser.BinaryExpr{},
			expected: &parser.ParenExpr{
				Expr: &parser.BinaryExpr{},
			},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			expr := wrapParenExpr(tc.expr)
			require.Equal(t, tc.expected, expr)
		})
	}
}

func TestKeepValueTypes(t *testing.T) {
	for i, tc := range []struct {
		input    []parser.ValueType
		keep     []parser.ValueType
		expected []parser.ValueType
	}{
		{
			input:    []parser.ValueType{},
			keep:     []parser.ValueType{},
			expected: []parser.ValueType{},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeString},
			keep:     []parser.ValueType{},
			expected: []parser.ValueType{},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeString},
			keep:     []parser.ValueType{parser.ValueTypeMatrix},
			expected: []parser.ValueType{},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeMatrix},
			keep:     []parser.ValueType{parser.ValueTypeMatrix},
			expected: []parser.ValueType{parser.ValueTypeMatrix},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeVector},
			keep:     []parser.ValueType{parser.ValueTypeMatrix},
			expected: []parser.ValueType{parser.ValueTypeMatrix},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeVector},
			keep:     []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar},
			expected: []parser.ValueType{parser.ValueTypeMatrix},
		},
		{
			input:    []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeVector},
			keep:     []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeVector},
			expected: []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeVector},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			res := keepValueTypes(tc.input, tc.keep)
			require.Equal(t, tc.expected, res)
		})
	}
}

func TestWalkBinaryOp(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	for i, tc := range []struct {
		disallowVector bool
		expectedFunc   func(op parser.ItemType)
	}{
		{
			disallowVector: true,
			expectedFunc: func(op parser.ItemType) {
				require.True(t, !op.IsSetOperator())
			},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			op := p.walkBinaryOp(tc.disallowVector)
			tc.expectedFunc(op)
		})
	}
}

func TestWalkMatrixSelector(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkMatrixSelector()
	ms, ok := expr.(*parser.MatrixSelector)
	require.True(t, ok)
	// We make sure the range generated is > 0.
	require.True(t, ms.Range > 0)
}

func TestWalkNumberLiteral(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkNumberLiteral()
	_, ok := expr.(*parser.NumberLiteral)
	require.True(t, ok)
}

func TestWalkUnaryExpr(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	for i, tc := range []struct {
		valueTypes   []parser.ValueType
		expectedFunc func(u *parser.UnaryExpr)
	}{
		{
			valueTypes:   []parser.ValueType{},
			expectedFunc: func(u *parser.UnaryExpr) {},
		},
		{
			valueTypes: []parser.ValueType{parser.ValueTypeScalar},
			expectedFunc: func(u *parser.UnaryExpr) {
				require.Equal(t, parser.ValueTypeScalar, u.Expr.Type())
			},
		},
		{
			valueTypes: []parser.ValueType{parser.ValueTypeVector},
			expectedFunc: func(u *parser.UnaryExpr) {
				require.Equal(t, parser.ValueTypeVector, u.Expr.Type())
			},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			expr := p.walkUnaryExpr(tc.valueTypes...)
			e, ok := expr.(*parser.UnaryExpr)
			require.Equal(t, parser.ItemType(parser.SUB), e.Op)
			require.True(t, ok)
			tc.expectedFunc(e)
		})
	}
}

func TestWalkSubQueryExpr(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkSubQueryExpr()
	e, ok := expr.(*parser.SubqueryExpr)
	require.True(t, ok)
	require.Equal(t, time.Hour, e.Range)
	require.Equal(t, time.Minute, e.Step)
	if e.StartOrEnd != 0 {
		require.True(t, e.StartOrEnd == parser.START || e.StartOrEnd == parser.END)
	}
}

func TestWalkFuncArgs(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	for _, f := range parser.Functions {
		// Skip string type arg function for now as we don't support it.
		if slices.Contains(f.ArgTypes, parser.ValueTypeString) {
			continue
		}
		call := &parser.Call{Func: f}
		p.walkFuncArgs(call)
		for i, arg := range call.Args {
			require.Equal(t, f.ArgTypes[i], arg.Type())
		}
	}
}

func TestWalkGrouping(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	for i, tc := range []struct {
		seriesMaps []map[string]string
	}{
		{
			seriesMaps: []map[string]string{},
		},
		{
			seriesMaps: []map[string]string{{"foo": "bar"}},
		},
		{
			seriesMaps: []map[string]string{{"foo": "bar", "test1": "test"}},
		},
		{
			seriesMaps: []map[string]string{{"foo": "bar", "test1": "test", "a": "b"}},
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			labelNames := make(map[string]struct{})
			for _, ss := range tc.seriesMaps {
				for k := range ss {
					labelNames[k] = struct{}{}
				}
			}
			seriesSet := make([]labels.Labels, len(tc.seriesMaps))
			for i, ss := range tc.seriesMaps {
				seriesSet[i] = labels.FromMap(ss)
			}
			p := New(rnd, seriesSet, opts...)
			grouping := p.walkGrouping()
			// We have a hardcoded grouping labels limit of 5.
			require.True(t, len(grouping) < maxGroupingLabels)
			for _, g := range grouping {
				_, ok := labelNames[g]
				require.True(t, ok)
			}
		})
	}
}

func TestWalkAggregateExpr(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkAggregateExpr()
	e, ok := expr.(*parser.AggregateExpr)
	require.True(t, ok)
	require.True(t, e.Op.IsAggregator())
	if e.Op.IsAggregatorWithParam() {
		require.True(t, e.Param != nil)
	}
}

func TestWalkVectorSelector(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := p.walkVectorSelector()
	vs, ok := expr.(*parser.VectorSelector)
	require.True(t, ok)
	for _, matcher := range vs.LabelMatchers {
		require.Equal(t, labels.MatchEqual, matcher.Type)
	}
}

func TestWalkLabelMatchers(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	for i, tc := range []struct {
		ss []labels.Labels
	}{
		{
			ss: nil,
		},
		{
			ss: []labels.Labels{labels.EmptyLabels()},
		},
		{
			ss: []labels.Labels{labels.FromStrings("foo", "bar")},
		},
		{
			ss: testSeriesSet,
		},
	} {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			p := New(rnd, tc.ss, opts...)
			matchers := p.walkLabelMatchers()
			for _, matcher := range matchers {
				require.Equal(t, labels.MatchEqual, matcher.Type)
			}
		})
	}
}

func TestWalkHoltWinters(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []Option{WithEnableOffset(true), WithEnableAtModifier(true)}
	p := New(rnd, testSeriesSet, opts...)
	expr := &parser.Call{
		Func: parser.Functions["holt_winters"],
		Args: make([]parser.Expr, len(parser.Functions["holt_winters"].ArgTypes)),
	}
	p.walkHoltWinters(expr)
	require.Equal(t, parser.ValueTypeMatrix, expr.Args[0].Type())
	s1, ok := expr.Args[1].(*parser.NumberLiteral)
	require.True(t, ok)
	require.True(t, s1.Val > 0 && s1.Val < 1)
	s2, ok := expr.Args[2].(*parser.NumberLiteral)
	require.True(t, ok)
	require.True(t, s2.Val > 0 && s2.Val < 1)
}
