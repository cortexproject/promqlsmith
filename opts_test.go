package promqlsmith

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestWithEnableOffset(t *testing.T) {
	o := &options{}
	WithEnableOffset(true).apply(o)
	require.True(t, o.enableOffset)
}

func TestWithEnableAtModifier(t *testing.T) {
	o := &options{}
	WithEnableAtModifier(true).apply(o)
	require.True(t, o.enableAtModifier)
}

func TestWithEnabledAggrs(t *testing.T) {
	o := &options{}
	WithEnabledAggrs([]parser.ItemType{parser.SUM}).apply(o)
	require.Equal(t, []parser.ItemType{parser.SUM}, o.enabledAggrs)
}

func TestWithEnabledBinOps(t *testing.T) {
	o := &options{}
	WithEnabledBinOps([]parser.ItemType{parser.ADD}).apply(o)
	require.Equal(t, []parser.ItemType{parser.ADD}, o.enabledBinops)
}

func TestWithEnabledExprs(t *testing.T) {
	o := &options{}
	WithEnabledExprs([]ExprType{VectorSelector}).apply(o)
	require.Equal(t, []ExprType{VectorSelector}, o.enabledExprs)
}

func TestWithEnabledFunctions(t *testing.T) {
	o := &options{}
	WithEnabledFunctions([]*parser.Function{parser.Functions["absent"]}).apply(o)
	require.Equal(t, []*parser.Function{parser.Functions["absent"]}, o.enabledFuncs)
}
