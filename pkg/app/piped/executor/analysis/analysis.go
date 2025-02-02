// Copyright 2020 The PipeCD Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	httpprovider "github.com/pipe-cd/pipe/pkg/app/piped/analysisprovider/http"
	"github.com/pipe-cd/pipe/pkg/app/piped/analysisprovider/log"
	logfactory "github.com/pipe-cd/pipe/pkg/app/piped/analysisprovider/log/factory"
	"github.com/pipe-cd/pipe/pkg/app/piped/analysisprovider/metrics"
	metricsfactory "github.com/pipe-cd/pipe/pkg/app/piped/analysisprovider/metrics/factory"
	"github.com/pipe-cd/pipe/pkg/app/piped/executor"
	"github.com/pipe-cd/pipe/pkg/config"
	"github.com/pipe-cd/pipe/pkg/model"
)

type Executor struct {
	executor.Input

	repoDir             string
	config              *config.Config
	startTime           time.Time
	previousElapsedTime time.Duration
}

type registerer interface {
	Register(stage model.Stage, f executor.Factory) error
}

func Register(r registerer) {
	f := func(in executor.Input) executor.Executor {
		return &Executor{
			Input: in,
		}
	}
	r.Register(model.StageAnalysis, f)
}

// templateArgs allows deployment-specific data to be embedded in the analysis template.
// NOTE: Changing its fields will force users to change the template definition.
type templateArgs struct {
	App struct {
		Name string
		Env  string
	}
	K8s struct {
		Namespace string
	}
	// User-defined custom args.
	Args map[string]string
}

// Execute spawns and runs multiple analyzer that run a query at the regular time.
// Any on of those fail then the stage ends with failure.
func (e *Executor) Execute(sig executor.StopSignal) model.StageStatus {
	e.startTime = time.Now()
	ctx := sig.Context()
	options := e.StageConfig.AnalysisStageOptions
	if options == nil {
		e.Logger.Error("missing analysis configuration for ANALYSIS stage")
		return model.StageStatus_STAGE_FAILURE
	}

	ds, err := e.RunningDSP.Get(ctx, e.LogPersister)
	if err != nil {
		e.LogPersister.Errorf("Failed to prepare running deploy source data (%v)", err)
		return model.StageStatus_STAGE_FAILURE
	}
	e.repoDir = ds.RepoDir
	e.config = ds.DeploymentConfig

	templateCfg, err := config.LoadAnalysisTemplate(e.repoDir)
	if errors.Is(err, config.ErrNotFound) {
		e.Logger.Info("config file for AnalysisTemplate not found")
		templateCfg = &config.AnalysisTemplateSpec{}
	} else if err != nil {
		e.LogPersister.Error(err.Error())
		return model.StageStatus_STAGE_FAILURE
	}

	timeout := time.Duration(options.Duration)
	e.previousElapsedTime = e.retrievePreviousElapsedTime()
	if e.previousElapsedTime > 0 {
		// Restart from the middle.
		timeout -= e.previousElapsedTime
	}
	defer e.saveElapsedTime(ctx)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	// Run analyses with metrics providers.
	for i := range options.Metrics {
		// TODO: Use metrics analyzer to perform ADA for each strategy
		analyzer, err := e.newAnalyzerForMetrics(i, &options.Metrics[i], templateCfg)
		if err != nil {
			e.LogPersister.Errorf("Failed to spawn analyzer for %s: %v", options.Metrics[i].Provider, err)
			return model.StageStatus_STAGE_FAILURE
		}
		eg.Go(func() error {
			e.LogPersister.Infof("[%s] Start analysis for %s", analyzer.id, analyzer.providerType)
			return analyzer.run(ctx)
		})
	}
	// Run analyses with logging providers.
	for i := range options.Logs {
		analyzer, err := e.newAnalyzerForLog(i, &options.Logs[i], templateCfg)
		if err != nil {
			e.LogPersister.Errorf("Failed to spawn analyzer for %s: %v", options.Logs[i].Provider, err)
			return model.StageStatus_STAGE_FAILURE
		}
		eg.Go(func() error {
			e.LogPersister.Infof("[%s] Start analysis for %s", analyzer.id, analyzer.providerType)
			return analyzer.run(ctx)
		})
	}
	// Run analyses with http providers.
	for i := range options.Https {
		analyzer, err := e.newAnalyzerForHTTP(i, &options.Https[i], templateCfg)
		if err != nil {
			e.LogPersister.Errorf("Failed to spawn analyzer for HTTP: %v", err)
			return model.StageStatus_STAGE_FAILURE
		}
		eg.Go(func() error {
			e.LogPersister.Infof("[%s] Start analysis for %s", analyzer.id, analyzer.providerType)
			return analyzer.run(ctx)
		})
	}

	if err := eg.Wait(); err != nil {
		e.LogPersister.Errorf("Analysis failed: %s", err.Error())
		return model.StageStatus_STAGE_FAILURE
	}

	status := executor.DetermineStageStatus(sig.Signal(), e.Stage.Status, model.StageStatus_STAGE_SUCCESS)
	if status != model.StageStatus_STAGE_SUCCESS {
		return status
	}

	e.LogPersister.Success("All analyses were successful")
	err = e.AnalysisResultStore.PutLatestAnalysisResult(ctx, &model.AnalysisResult{
		StartTime: e.startTime.Unix(),
	})
	if err != nil {
		e.Logger.Error("failed to send the analysis metadata")
	}
	return status
}

const elapsedTimeKey = "elapsedTime"

// saveElapsedTime stores the elapsed time of analysis stage into metadata persister.
// The analysis stage can be restarted from the middle even if it ends unexpectedly,
// that's why count should be stored.
func (e *Executor) saveElapsedTime(ctx context.Context) {
	elapsedTime := time.Since(e.startTime) + e.previousElapsedTime
	metadata := map[string]string{
		elapsedTimeKey: elapsedTime.String(),
	}
	if err := e.MetadataStore.SetStageMetadata(ctx, e.Stage.Id, metadata); err != nil {
		e.Logger.Error("failed to store metadata", zap.Error(err))
	}
}

// retrievePreviousElapsedTime sets the elapsed time of analysis stage by decoding metadata.
func (e *Executor) retrievePreviousElapsedTime() time.Duration {
	metadata, ok := e.MetadataStore.GetStageMetadata(e.Stage.Id)
	if !ok {
		return 0
	}
	s, ok := metadata[elapsedTimeKey]
	if !ok {
		return 0
	}
	et, err := time.ParseDuration(s)
	if err != nil {
		e.Logger.Error("unexpected elapsed time is stored", zap.String("stored-value", s), zap.Error(err))
		return 0
	}
	return et
}

func (e *Executor) newAnalyzerForMetrics(i int, templatable *config.TemplatableAnalysisMetrics, templateCfg *config.AnalysisTemplateSpec) (*analyzer, error) {
	cfg, err := e.getMetricsConfig(templatable, templateCfg, templatable.Template.Args)
	if err != nil {
		return nil, err
	}
	provider, err := e.newMetricsProvider(cfg.Provider, templatable)
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("metrics-%d", i)
	runner := func(ctx context.Context, query string) (bool, string, error) {
		now := time.Now()
		queryRange := metrics.QueryRange{
			From: now.Add(-cfg.Interval.Duration()),
			To:   now,
		}
		return provider.Evaluate(ctx, query, queryRange, &cfg.Expected)
	}
	return newAnalyzer(id, provider.Type(), cfg.Query, runner, time.Duration(cfg.Interval), cfg.FailureLimit, cfg.SkipOnNoData, e.Logger, e.LogPersister), nil
}

func (e *Executor) newAnalyzerForLog(i int, templatable *config.TemplatableAnalysisLog, templateCfg *config.AnalysisTemplateSpec) (*analyzer, error) {
	cfg, err := e.getLogConfig(templatable, templateCfg, templatable.Template.Args)
	if err != nil {
		return nil, err
	}
	provider, err := e.newLogProvider(cfg.Provider)
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("log-%d", i)
	runner := func(ctx context.Context, query string) (bool, string, error) {
		return provider.Evaluate(ctx, query)
	}
	return newAnalyzer(id, provider.Type(), cfg.Query, runner, time.Duration(cfg.Interval), cfg.FailureLimit, cfg.SkipOnNoData, e.Logger, e.LogPersister), nil
}

func (e *Executor) newAnalyzerForHTTP(i int, templatable *config.TemplatableAnalysisHTTP, templateCfg *config.AnalysisTemplateSpec) (*analyzer, error) {
	cfg, err := e.getHTTPConfig(templatable, templateCfg, templatable.Template.Args)
	if err != nil {
		return nil, err
	}
	provider := httpprovider.NewProvider(time.Duration(cfg.Timeout))
	id := fmt.Sprintf("http-%d", i)
	runner := func(ctx context.Context, query string) (bool, string, error) {
		return provider.Run(ctx, cfg)
	}
	return newAnalyzer(id, provider.Type(), "", runner, time.Duration(cfg.Interval), cfg.FailureLimit, cfg.SkipOnNoData, e.Logger, e.LogPersister), nil
}

func (e *Executor) newMetricsProvider(providerName string, templatable *config.TemplatableAnalysisMetrics) (metrics.Provider, error) {
	cfg, ok := e.PipedConfig.GetAnalysisProvider(providerName)
	if !ok {
		return nil, fmt.Errorf("unknown provider name %s", providerName)
	}
	provider, err := metricsfactory.NewProvider(templatable, &cfg, e.Logger)
	if err != nil {
		return nil, err
	}
	return provider, nil
}

func (e *Executor) newLogProvider(providerName string) (log.Provider, error) {
	cfg, ok := e.PipedConfig.GetAnalysisProvider(providerName)
	if !ok {
		return nil, fmt.Errorf("unknown provider name %s", providerName)
	}
	provider, err := logfactory.NewProvider(&cfg, e.Logger)
	if err != nil {
		return nil, err
	}
	return provider, nil
}

// getMetricsConfig renders the given template and returns the metrics config.
// Just returns metrics config if no template specified.
func (e *Executor) getMetricsConfig(templatableCfg *config.TemplatableAnalysisMetrics, templateCfg *config.AnalysisTemplateSpec, args map[string]string) (*config.AnalysisMetrics, error) {
	name := templatableCfg.Template.Name
	if name == "" {
		cfg := &templatableCfg.AnalysisMetrics
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid metrics configuration: %w", err)
		}
		return cfg, nil
	}

	var err error
	templateCfg, err = e.render(*templateCfg, args)
	if err != nil {
		return nil, err
	}
	cfg, ok := templateCfg.Metrics[name]
	if !ok {
		return nil, fmt.Errorf("analysis template %s not found despite template specified", name)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid metrics configuration: %w", err)
	}
	return &cfg, nil
}

// getLogConfig renders the given template and returns the log config.
// Just returns log config if no template specified.
func (e *Executor) getLogConfig(templatableCfg *config.TemplatableAnalysisLog, templateCfg *config.AnalysisTemplateSpec, args map[string]string) (*config.AnalysisLog, error) {
	name := templatableCfg.Template.Name
	if name == "" {
		return &templatableCfg.AnalysisLog, nil
	}

	var err error
	templateCfg, err = e.render(*templateCfg, args)
	if err != nil {
		return nil, err
	}
	cfg, ok := templateCfg.Logs[name]
	if !ok {
		return nil, fmt.Errorf("analysis template %s not found despite template specified", name)
	}
	return &cfg, nil
}

// getHTTPConfig renders the given template and returns the http config.
// Just returns http config if no template specified.
func (e *Executor) getHTTPConfig(templatableCfg *config.TemplatableAnalysisHTTP, templateCfg *config.AnalysisTemplateSpec, args map[string]string) (*config.AnalysisHTTP, error) {
	name := templatableCfg.Template.Name
	if name == "" {
		return &templatableCfg.AnalysisHTTP, nil
	}

	var err error
	templateCfg, err = e.render(*templateCfg, args)
	if err != nil {
		return nil, err
	}
	cfg, ok := templateCfg.HTTPs[name]
	if !ok {
		return nil, fmt.Errorf("analysis template %s not found despite template specified", name)
	}
	return &cfg, nil
}

// render returns a new AnalysisTemplateSpec, where deployment-specific arguments populated.
//
// TODO: Change Template Args reference name
//   Use .BuiltInArgs.App.Name instead of .App.Name
//   Besides, we'd prefer to keep the variables for variant as is.
func (e *Executor) render(templateCfg config.AnalysisTemplateSpec, customArgs map[string]string) (*config.AnalysisTemplateSpec, error) {
	args := templateArgs{
		Args: customArgs,
		App: struct {
			Name string
			Env  string
			// TODO: Populate Env
		}{Name: e.Application.Name, Env: ""},
	}
	if e.config.Kind == config.KindKubernetesApp {
		namespace := "default"
		if n := e.config.KubernetesDeploymentSpec.Input.Namespace; n != "" {
			namespace = n
		}
		args.K8s = struct{ Namespace string }{Namespace: namespace}
	}

	cfg, err := json.Marshal(templateCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}
	t, err := template.New("AnalysisTemplate").Parse(string(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse text: %w", err)
	}
	b := new(bytes.Buffer)
	if err := t.Execute(b, args); err != nil {
		return nil, fmt.Errorf("failed to apply template: %w", err)
	}
	newCfg := &config.AnalysisTemplateSpec{}
	err = json.Unmarshal(b.Bytes(), newCfg)
	return newCfg, err
}
