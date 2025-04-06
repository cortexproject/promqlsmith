package promqlsmith

import (
	"testing"
	"time"

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

func TestWithEnableExperimentalPromQL(t *testing.T) {
	o := &options{}
	WithEnableExperimentalPromQLFunctions(true).apply(o)
	WithEnabledFunctions(nil).apply(o)
	WithEnabledAggrs(nil).apply(o)
	o.applyDefaults()

	// check experimental aggrs and funcs are appended well
	require.True(t, o.enableExperimentalPromQLFunctions)
	require.Equal(t, len(defaultSupportedAggrs)+len(experimentalPromQLAggrs), len(o.enabledAggrs))
	require.Equal(t, len(defaultSupportedFuncs)+len(experimentalSupportedFuncs), len(o.enabledFuncs))
}

func TestWithEnabledAggrs(t *testing.T) {
	o := &options{}
	WithEnabledAggrs([]parser.ItemType{parser.SUM}).apply(o)
	require.Equal(t, []parser.ItemType{parser.SUM}, o.enabledAggrs)
}

func TestWithMaxAtModifierTimestamp(t *testing.T) {
	o := &options{}
	o.applyDefaults()
	require.GreaterOrEqual(t, time.Now().UnixMilli(), o.atModifierMaxTimestamp)
	WithAtModifierMaxTimestamp(time.UnixMilli(1000).UnixMilli()).apply(o)
	require.Equal(t, int64(1000), o.atModifierMaxTimestamp)
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

func TestWithMaxDepth(t *testing.T) {
	o := &options{}
	WithMaxDepth(3).apply(o)
	require.Equal(t, 3, o.maxDepth)

	// Test default value
	o = &options{}
	o.applyDefaults()
	require.Equal(t, 5, o.maxDepth) // Default depth
}
