package analysis

import (
	"context"
	"fmt"
	"time"

	"github.com/pipe-cd/pipe/pkg/config"
)

type dynamicMetricsStrategy string

const (
	strategyCanaryWithBaseline dynamicMetricsStrategy = "CANARY_WITH_BASELINE"
	strategyCanaryWithPrimary  dynamicMetricsStrategy = "CANARY_WITH_PRIMARY"
	strategyPrevious           dynamicMetricsStrategy = "PREVIOUS"
)

type dynamicMetricsAnalyzer struct {
	strategy dynamicMetricsStrategy
	cfg      config.AnalysisMetrics

	primaryArgs  map[string]string
	canaryArgs   map[string]string
	baselineArgs map[string]string
}

func newDynamicMetricsAnalyzer(strategy dynamicMetricsStrategy, templateRef config.AnalysisDynamicMetrics, templates *config.AnalysisTemplateSpec) (*dynamicMetricsAnalyzer, error) {
	cfg, ok := templates.Metrics[templateRef.Template]
	if !ok {
		return nil, fmt.Errorf("template %q not found", templateRef.Template)
	}
	return &dynamicMetricsAnalyzer{
		strategy:     strategy,
		cfg:          cfg,
		primaryArgs:  templateRef.PrimaryArgs,
		canaryArgs:   templateRef.CanaryArgs,
		baselineArgs: templateRef.BaselineArgs,
	}, nil
}

func (d *dynamicMetricsAnalyzer) run(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.Interval.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			switch d.strategy {
			case strategyPrevious:
				// FIXME: それぞれのクエリを生成して、それぞれの結果セットを比較する。そのために、analysis providersはデータポイントを返すだけの仕様に変更する必要あるかも
			case strategyCanaryWithBaseline:
			case strategyCanaryWithPrimary:
			}
		case <-ctx.Done():
			return nil
		}
	}
}
