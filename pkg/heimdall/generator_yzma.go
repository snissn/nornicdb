//go:build windows || yzma

// Package heimdall provides the Heimdall cognitive guardian for NornicDB.
// This file provides the yzma-enabled generator for Windows GPU support.
package heimdall

import (
	"context"
	"os"
	"strconv"

	"github.com/orneryd/nornicdb/pkg/localllm"
)

func init() {
	// Register the yzma-enabled generator loader for Windows
	SetGeneratorLoader(yzmaGeneratorLoader)
}

// yzmaGeneratorLoader loads a generation model using yzma bindings (Windows GPU).
func yzmaGeneratorLoader(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
	opts := localllm.DefaultGenerationOptions(modelPath)
	opts.GPULayers = gpuLayers
	opts.ContextSize = contextSize
	opts.BatchSize = batchSize

	// Apply Heimdall-specific context features from env
	if v := yzmaEnvInt("NORNICDB_HEIMDALL_CTX_TYPE"); v != 0 {
		opts.Features.CtxType = v
	}
	if v := yzmaEnvInt("NORNICDB_HEIMDALL_POOLING_TYPE"); v != 0 {
		opts.Features.PoolingType = v
	}
	if v := yzmaEnvInt("NORNICDB_HEIMDALL_ATTENTION_TYPE"); v != 0 {
		opts.Features.AttentionType = v
	}
	if v := yzmaEnvInt("NORNICDB_HEIMDALL_FLASH_ATTN"); v != 0 {
		opts.Features.FlashAttn = v
	}

	model, err := localllm.LoadGenerationModel(opts)
	if err != nil {
		return nil, err
	}

	return &yzmaGenerator{model: model}, nil
}

// yzmaEnvInt reads an env var as int, returns 0 if unset or invalid.
func yzmaEnvInt(key string) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

// yzmaGenerator wraps localllm.GenerationModel to implement Generator interface.
type yzmaGenerator struct {
	model *localllm.GenerationModel
}

func (g *yzmaGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   int32(params.MaxTokens),
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        int32(params.TopK),
	}
	return g.model.Generate(ctx, prompt, llamaParams)
}

func (g *yzmaGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   int32(params.MaxTokens),
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        int32(params.TopK),
	}
	return g.model.GenerateStream(ctx, prompt, llamaParams, callback)
}

func (g *yzmaGenerator) Close() error {
	return g.model.Close()
}

func (g *yzmaGenerator) ModelPath() string {
	return g.model.ModelPath()
}
