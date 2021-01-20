// Copyright 2021 The PipeCD Authors.
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

package lambda

import (
	"context"

	"github.com/pipe-cd/pipe/pkg/app/piped/deploysource"
	"github.com/pipe-cd/pipe/pkg/app/piped/executor"
	"github.com/pipe-cd/pipe/pkg/config"
	"github.com/pipe-cd/pipe/pkg/model"
)

type deployExecutor struct {
	executor.Input

	deploySource      *deploysource.DeploySource
	deployCfg         *config.LambdaDeploymentSpec
	cloudProviderName string
	cloudProviderCfg  *config.CloudProviderLambdaConfig
}

func (e *deployExecutor) Execute(sig executor.StopSignal) model.StageStatus {
	ctx := sig.Context()
	ds, err := e.TargetDSP.GetReadOnly(ctx, e.LogPersister)
	if err != nil {
		e.LogPersister.Errorf("Failed to prepare target deploy source data (%v)", err)
		return model.StageStatus_STAGE_FAILURE
	}

	e.deploySource = ds
	e.deployCfg = ds.DeploymentConfig.LambdaDeploymentSpec
	if e.deployCfg == nil {
		e.LogPersister.Errorf("Malformed deployment configuration: missing LambdaDeploymentSpec")
		return model.StageStatus_STAGE_FAILURE
	}

	var found bool
	e.cloudProviderName, e.cloudProviderCfg, found = findCloudProvider(&e.Input)
	if !found {
		return model.StageStatus_STAGE_FAILURE
	}

	var (
		originalStatus = e.Stage.Status
		status         model.StageStatus
	)

	switch model.Stage(e.Stage.Name) {
	case model.StageLambdaSync:
		status = e.ensureSync(ctx)
	case model.StageLambdaPromote:
		status = e.ensurePromote(ctx)
	default:
		e.LogPersister.Errorf("Unsupported stage %s for lambda application", e.Stage.Name)
		return model.StageStatus_STAGE_FAILURE
	}

	return executor.DetermineStageStatus(sig.Signal(), originalStatus, status)
}

func (e *deployExecutor) ensureSync(ctx context.Context) model.StageStatus {
	fm, ok := loadFunctionManifest(&e.Input, e.deployCfg.Input.FunctionManifestFile, e.deploySource)
	if !ok {
		return model.StageStatus_STAGE_FAILURE
	}

	if !sync(ctx, &e.Input, e.cloudProviderName, e.cloudProviderCfg, fm) {
		return model.StageStatus_STAGE_FAILURE
	}

	return model.StageStatus_STAGE_SUCCESS
}

func (e *deployExecutor) ensurePromote(_ context.Context) model.StageStatus {
	e.LogPersister.Error("This stage is not implemented yet")
	return model.StageStatus_STAGE_FAILURE
}