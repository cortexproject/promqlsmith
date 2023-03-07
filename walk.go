package promqlsmith

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

// walkExpr generates the given expression type with one of the required value type.
// valueTypes is only used for expressions that could have multiple possible return value types.
func (s *PromQLSmith) walkExpr(e ExprType, valueTypes ...parser.ValueType) (parser.Expr, error) {
	switch e {
	case AggregateExpr:
		return s.walkAggregateExpr(), nil
	case BinaryExpr:
		// Wrap binary expression with paren for readability.
		return wrapParenExpr(s.walkBinaryExpr(valueTypes...)), nil
	case SubQueryExpr:
		return s.walkSubQueryExpr(), nil
	case MatrixSelector:
		return s.walkMatrixSelector(), nil
	case VectorSelector:
		vs, _ := s.walkVectorSelector()
		return vs, nil
	case CallExpr:
		return s.walkCall(valueTypes...), nil
	case NumberLiteral:
		return s.walkNumberLiteral(), nil
	case UnaryExpr:
		return s.walkUnaryExpr(valueTypes...), nil
	default:
		return nil, fmt.Errorf("unsupported ExprType %d", e)
	}
}

func (s *PromQLSmith) walkAggregateExpr() parser.Expr {
	// TODO: support other vector value types.
	vs, series := s.walkVectorSelector()
	expr := &parser.AggregateExpr{
		Op:       s.supportedAggrs[s.rnd.Intn(len(s.supportedAggrs))],
		Without:  s.rnd.Int()%2 == 0,
		Expr:     vs,
		Grouping: s.walkGrouping(series),
	}
	if expr.Op.IsAggregatorWithParam() {
		expr.Param = s.walkAggregateParam(expr.Op)
	}
	return expr
}

func (s *PromQLSmith) walkGrouping(series labels.Labels) []string {
	orders := s.rnd.Perm(series.Len())
	items := s.rnd.Intn(series.Len() + 1)
	res := make([]string, items)
	for i := 0; i < items; i++ {
		res[i] = series[orders[i]].Name
	}
	return res
}

func (s *PromQLSmith) walkAggregateParam(op parser.ItemType) parser.Expr {
	switch op {
	case parser.TOPK, parser.BOTTOMK:
		// This can be nested as well, but we just number literal for simplicity.
		return &parser.NumberLiteral{Val: float64(s.rnd.Intn(100) + 1)}
	case parser.QUANTILE:
		// This can be nested as well, but we just number literal for simplicity.
		return &parser.NumberLiteral{Val: s.rnd.Float64()}
	case parser.COUNT_VALUES:
		return &parser.StringLiteral{Val: "value"}
	}
	return nil
}

// Can only do binary expression between vector and scalar. So any expression
// that returns matrix doesn't work like matrix selector, subquery
// or function that returns matrix.
func (s *PromQLSmith) walkBinaryExpr(valueTypes ...parser.ValueType) parser.Expr {
	valueTypes = keepValueTypes(valueTypes, vectorAndScalarValueTypes)
	if len(valueTypes) == 0 {
		valueTypes = vectorAndScalarValueTypes
	}
	expr := &parser.BinaryExpr{
		Op:             s.walkBinaryOp(!slices.Contains(valueTypes, parser.ValueTypeVector)),
		VectorMatching: &parser.VectorMatching{},
	}
	// If it is a set operator then only vectors are allowed.
	if expr.Op.IsSetOperator() {
		valueTypes = []parser.ValueType{parser.ValueTypeVector}
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	expr.LHS = wrapParenExpr(s.Walk(valueTypes...))
	expr.RHS = wrapParenExpr(s.Walk(valueTypes...))
	lvt := expr.LHS.Type()
	rvt := expr.RHS.Type()
	// ReturnBool can only be set for comparison operator. It is
	// required to set to true if both expressions are scalar type.
	if expr.Op.IsComparisonOperator() {
		if lvt == parser.ValueTypeScalar && rvt == parser.ValueTypeScalar || s.rnd.Intn(2) == 0 {
			expr.ReturnBool = true
		}
	}
	return expr
}

// TODO: support vector matching types.
func (s *PromQLSmith) walkBinaryVectorMatching(expr *parser.BinaryExpr) {

}

// Walk binary op based on whether vector value type is allowed or not.
// Since Set operator only works with vector so if vector is disallowed
// we will choose comparison operator that works both for scalar and vector.
func (s *PromQLSmith) walkBinaryOp(disallowVector bool) parser.ItemType {
	binops := s.supportedBinops
	if disallowVector {
		binops = make([]parser.ItemType, 0)
		for _, binop := range s.supportedBinops {
			// Set operator can only be used with vector operator.
			if binop.IsSetOperator() {
				continue
			}
			binops = append(binops, binop)
		}
	}
	return binops[s.rnd.Intn(len(binops))]
}

func (s *PromQLSmith) walkSubQueryExpr() parser.Expr {
	vs, _ := s.walkVectorSelector()
	expr := &parser.SubqueryExpr{
		Range: time.Hour,
		Step:  time.Minute,
		Expr:  vs,
	}
	if s.enableOffset && s.rnd.Int()%2 == 0 {
		negativeOffset := s.rnd.Intn(2) == 0
		expr.OriginalOffset = time.Duration(s.rnd.Intn(300)) * time.Second
		if negativeOffset {
			expr.OriginalOffset = -expr.OriginalOffset
		}
	}
	if s.enableAtModifier && s.rnd.Float64() > 0.7 {
		expr.Timestamp, expr.StartOrEnd = s.walkAtModifier()
	}
	return expr
}

func (s *PromQLSmith) walkCall(valueTypes ...parser.ValueType) parser.Expr {
	expr := &parser.Call{}

	funcs := s.supportedFuncs
	if len(valueTypes) > 0 {
		funcs = make([]*parser.Function, 0)
		valueTypeSet := make(map[parser.ValueType]struct{})
		for _, vt := range valueTypes {
			valueTypeSet[vt] = struct{}{}
		}
		for _, f := range s.supportedFuncs {
			if _, ok := valueTypeSet[f.ReturnType]; ok {
				funcs = append(funcs, f)
			}
		}
	}
	expr.Func = funcs[s.rnd.Intn(len(funcs))]
	s.walkFuncArgs(expr)
	return expr
}

func (s *PromQLSmith) walkFuncArgs(expr *parser.Call) {
	expr.Args = make([]parser.Expr, len(expr.Func.ArgTypes))
	for i, arg := range expr.Func.ArgTypes {
		expr.Args[i] = s.Walk(arg)
	}
}

func (s *PromQLSmith) walkVectorSelector() (parser.Expr, labels.Labels) {
	expr := &parser.VectorSelector{}
	var series labels.Labels
	expr.LabelMatchers, series = s.walkLabelMatchers()
	if s.enableOffset && s.rnd.Int()%2 == 0 {
		negativeOffset := s.rnd.Intn(2) == 0
		expr.OriginalOffset = time.Duration(s.rnd.Intn(300)) * time.Second
		if negativeOffset {
			expr.OriginalOffset = -expr.OriginalOffset
		}
	}
	if s.enableAtModifier && s.rnd.Float64() > 0.7 {
		expr.Timestamp, expr.StartOrEnd = s.walkAtModifier()
	}

	return expr, series
}

func (s *PromQLSmith) walkLabelMatchers() ([]*labels.Matcher, labels.Labels) {
	series := s.seriesSet[s.rnd.Intn(len(s.seriesSet))]
	orders := s.rnd.Perm(series.Len())
	items := s.rnd.Intn(series.Len()/2) + 1
	matchers := make([]*labels.Matcher, items)
	for i := 0; i < items; i++ {
		matchers[i] = labels.MustNewMatcher(labels.MatchEqual, series[orders[i]].Name, series[orders[i]].Value)
	}
	return matchers, series
}

func (s *PromQLSmith) walkAtModifier() (ts *int64, op parser.ItemType) {
	res := s.rnd.Intn(3)
	switch res {
	case 0:
		op = parser.START
	case 1:
		op = parser.END
	case 2:
		t := time.Now().UnixMilli()
		ts = &t
	}
	return
}

func (s *PromQLSmith) walkMatrixSelector() parser.Expr {
	vs, _ := s.walkVectorSelector()
	return &parser.MatrixSelector{
		// Make sure the time range is > 0s.
		Range:          time.Duration(s.rnd.Intn(5)+1) * time.Minute,
		VectorSelector: vs,
	}
}

// Only vector and scalar result is allowed.
func (s *PromQLSmith) walkUnaryExpr(valueTypes ...parser.ValueType) parser.Expr {
	expr := &parser.UnaryExpr{
		Op: parser.SUB,
	}
	valueTypes = keepValueTypes(valueTypes, vectorAndScalarValueTypes)
	expr.Expr = s.Walk(valueTypes...)
	return expr
}

func (s *PromQLSmith) walkNumberLiteral() parser.Expr {
	return &parser.NumberLiteral{Val: s.rnd.Float64()}
}

func exprsFromValueTypes(valueTypes []parser.ValueType) []ExprType {
	set := make(map[ExprType]struct{})
	res := make([]ExprType, 0)
	for _, vt := range valueTypes {
		exprs, ok := valueTypeToExprsMap[vt]
		if !ok {
			continue
		}
		for _, expr := range exprs {
			set[expr] = struct{}{}
		}
	}
	for expr := range set {
		res = append(res, expr)
	}
	return res
}

// wrapParenExpr makes binary expr in a paren expr for better readability.
func wrapParenExpr(expr parser.Expr) parser.Expr {
	if _, ok := expr.(*parser.BinaryExpr); ok {
		return &parser.ParenExpr{Expr: expr}
	}
	return expr
}

// keepValueTypes picks value types that we should keep from the input.
// input shouldn't contain duplicate value types.
func keepValueTypes(input []parser.ValueType, keep []parser.ValueType) []parser.ValueType {
	out := make([]parser.ValueType, 0, len(keep))
	s := make(map[parser.ValueType]struct{})
	for _, vt := range keep {
		s[vt] = struct{}{}
	}
	for _, vt := range input {
		if _, ok := s[vt]; ok {
			out = append(out, vt)
		}
	}
	return out
}
