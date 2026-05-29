// Package localllm — shared types available on all platforms (no build tags).

package localllm

// ContextFeatures configures llama.cpp context parameters that vary by model.
//
// These are passthrough settings from environment variables so that different
// models (embedding, MTP, generation) can declare the features they need.
//
// Environment variables (per domain):
//
//	Embedding:  NORNICDB_EMBEDDING_CTX_TYPE, _POOLING_TYPE, _ATTENTION_TYPE, _FLASH_ATTN
//	Rerank:     NORNICDB_RERANK_CTX_TYPE,    _POOLING_TYPE, _ATTENTION_TYPE, _FLASH_ATTN
//	Heimdall:   NORNICDB_HEIMDALL_CTX_TYPE,  _POOLING_TYPE, _ATTENTION_TYPE, _FLASH_ATTN
type ContextFeatures struct {
	// CtxType selects the context type. 0=default, 1=MTP.
	// Only set to 1 if the model has MTP layers.
	CtxType int
	// PoolingType controls embedding pooling. 1=mean (default for embeddings),
	// 2=cls, 3=last, 4=rank. Use -1 to leave unspecified.
	PoolingType int
	// AttentionType controls attention masking. 0=causal (LLM default),
	// 1=non-causal (BERT-style, default for embeddings).
	AttentionType int
	// FlashAttn controls flash attention. -1=auto (default), 0=disabled, 1=enabled.
	FlashAttn int
}

// DefaultContextFeatures returns defaults optimized for embedding models.
func DefaultContextFeatures() ContextFeatures {
	return ContextFeatures{
		CtxType:       0,  // LLAMA_CONTEXT_TYPE_DEFAULT
		PoolingType:   1,  // LLAMA_POOLING_TYPE_MEAN
		AttentionType: 1,  // LLAMA_ATTENTION_TYPE_NON_CAUSAL
		FlashAttn:     -1, // LLAMA_FLASH_ATTN_TYPE_AUTO
	}
}
