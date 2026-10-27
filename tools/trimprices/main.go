//go:build ignore

// trimprices builds prices-snapshot.json, the price table embedded in the
// plugin, from a LiteLLM model_prices_and_context_window.json at a pinned
// commit. It keeps chat/completion/responses models of the providers the
// ledger sees and only the per-token price fields prices.go reads.
//
//	curl -sSfo /tmp/litellm/prices.json \
//	  https://raw.githubusercontent.com/BerriAI/litellm/<commit>/model_prices_and_context_window.json
//	go run tools/trimprices/main.go /tmp/litellm/prices.json <commit> <YYYY-MM-DD> > prices-snapshot.json
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
)

var (
	providers = map[string]bool{
		"anthropic": true, "openai": true, "chatgpt": true, "gemini": true,
		"vertex_ai-language-models": true, "vertex_ai-anthropic_models": true,
		"xai": true, "moonshot": true, "deepseek": true,
	}
	modes = map[string]bool{"": true, "chat": true, "completion": true, "responses": true}
	keep  = regexp.MustCompile(`^(litellm_provider|mode|output_cost_per_reasoning_token|cache_creation_input_token_cost_above_1hr|` +
		`(input_cost_per_token|output_cost_per_token|cache_read_input_token_cost|cache_creation_input_token_cost)` +
		`(_above_\d+k_tokens|_priority|_flex)?)$`)
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: trimprices <litellm.json> <commit> <as-of-date>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var doc map[string]map[string]json.RawMessage
	var loose map[string]json.RawMessage
	if err := json.Unmarshal(raw, &loose); err != nil {
		panic(err)
	}
	doc = map[string]map[string]json.RawMessage{}
	for name, entryRaw := range loose {
		var entry map[string]json.RawMessage
		if json.Unmarshal(entryRaw, &entry) != nil {
			continue
		}
		var provider, mode string
		_ = json.Unmarshal(entry["litellm_provider"], &provider)
		_ = json.Unmarshal(entry["mode"], &mode)
		if !providers[provider] || !modes[mode] {
			continue
		}
		trimmed := map[string]json.RawMessage{}
		for field, value := range entry {
			if keep.MatchString(field) {
				trimmed[field] = value
			}
		}
		if _, ok := trimmed["input_cost_per_token"]; !ok {
			if _, ok := trimmed["output_cost_per_token"]; !ok {
				continue
			}
		}
		doc[name] = trimmed
	}
	names := make([]string, 0, len(doc))
	for name := range doc {
		names = append(names, name)
	}
	sort.Strings(names)
	var buf bytes.Buffer
	commit, asOf := os.Args[2], os.Args[3]
	header, _ := json.Marshal(map[string]string{
		"source":     "LiteLLM model_prices_and_context_window.json, trimmed by tools/trimprices",
		"source_url": "https://raw.githubusercontent.com/BerriAI/litellm/" + commit + "/model_prices_and_context_window.json",
		"commit":     commit,
		"as_of":      asOf,
	})
	buf.Write(header[:len(header)-1])
	buf.WriteString(`,"models":{` + "\n")
	for i, name := range names {
		key, _ := json.Marshal(name)
		value, _ := json.Marshal(doc[name]) // map keys are sorted
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(value)
		if i < len(names)-1 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
	}
	buf.WriteString("}}\n")
	os.Stdout.Write(buf.Bytes())
	fmt.Fprintf(os.Stderr, "%d models, %d bytes\n", len(names), buf.Len())
}
