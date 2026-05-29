//go:build cgo && !nolocalllm && (darwin || linux)

// Package heimdall provides the Heimdall cognitive guardian for NornicDB.
// This file provides the CGO-enabled generator using localllm.
package heimdall

import (
	"context"
	"os"
	"strconv"

	"github.com/orneryd/nornicdb/pkg/localllm"
)

func init() {
	// Register the CGO-enabled generator loader
	SetGeneratorLoader(cgoGeneratorLoader)
}

// cgoGeneratorLoader loads a generation model using localllm CGO bindings.
func cgoGeneratorLoader(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
	opts := localllm.DefaultGenerationOptions(modelPath)
	opts.GPULayers = gpuLayers
	opts.ContextSize = contextSize
	opts.BatchSize = batchSize

	// Apply Heimdall-specific context features from env
	if v := envInt("NORNICDB_HEIMDALL_CTX_TYPE"); v != 0 {
		opts.Features.CtxType = v
	}
	if v := envInt("NORNICDB_HEIMDALL_POOLING_TYPE"); v != 0 {
		opts.Features.PoolingType = v
	}
	if v := envInt("NORNICDB_HEIMDALL_ATTENTION_TYPE"); v != 0 {
		opts.Features.AttentionType = v
	}
	if v := envInt("NORNICDB_HEIMDALL_FLASH_ATTN"); v != 0 {
		opts.Features.FlashAttn = v
	}

	model, err := localllm.LoadGenerationModel(opts)
	if err != nil {
		return nil, err
	}

	return &cgoGenerator{model: model}, nil
}

// envInt reads an env var as int, returns 0 if unset or invalid.
func envInt(key string) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

// cgoGenerator wraps localllm.GenerationModel to implement Generator interface.
type cgoGenerator struct {
	model *localllm.GenerationModel
}

func (g *cgoGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        params.TopK,
		StopTokens:  params.StopTokens,
	}
	return g.model.Generate(ctx, prompt, llamaParams)
}

func (g *cgoGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	llamaParams := localllm.GenerateParams{
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
		TopP:        params.TopP,
		TopK:        params.TopK,
		StopTokens:  params.StopTokens,
	}
	return g.model.GenerateStream(ctx, prompt, llamaParams, callback)
}

func (g *cgoGenerator) Close() error {
	return g.model.Close()
}

func (g *cgoGenerator) ModelPath() string {
	return g.model.ModelPath()
}
