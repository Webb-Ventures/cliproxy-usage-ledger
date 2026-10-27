package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Settings are the ledger's shared overrides, stored as one JSON document in
// the meta table (key "settings") so every browser sees the same numbers.
// Retention is not a setting: it belongs to the CPA plugin config and is
// returned read-only.

const (
	defaultCurrency       = "USD"
	maxSettingsEntries    = 2000
	maxSettingsKeyLength  = 256
	maxSettingsNameLength = 64
	// maxPerTokenPrice rejects values that are clearly per-million prices
	// entered as per-token prices ($1 per token is $1M per MTok).
	maxPerTokenPrice = 1.0
	maxMonthlyPrice  = 1_000_000.0
)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

type planOverride struct {
	Provider     string  `json:"provider"`
	Plan         string  `json:"plan"`
	MonthlyPrice float64 `json:"monthly_price"`
	Currency     string  `json:"currency"`
}

// ledgerSettings is the persisted document, also served as prices.overrides.
type ledgerSettings struct {
	PlanOverrides       map[string]planOverride `json:"plan_overrides"`
	ModelPriceOverrides map[string]tokenPrices  `json:"model_price_overrides"`
	DefaultCurrency     string                  `json:"default_currency"`
	UpdatedAt           *string                 `json:"updated_at"`
}

func defaultSettings() ledgerSettings {
	return ledgerSettings{
		PlanOverrides:       map[string]planOverride{},
		ModelPriceOverrides: map[string]tokenPrices{},
		DefaultCurrency:     defaultCurrency,
	}
}

type settingsResponse struct {
	Schema int `json:"schema"`
	ledgerSettings
	Retention statusRetention `json:"retention"`
}

// settingsPutBody is the PUT body. schema, retention, and updated_at are
// accepted (so a GET document can be sent back) and ignored.
type settingsPutBody struct {
	PlanOverrides       map[string]planOverride `json:"plan_overrides"`
	ModelPriceOverrides map[string]tokenPrices  `json:"model_price_overrides"`
	DefaultCurrency     *string                 `json:"default_currency"`
	Schema              json.RawMessage         `json:"schema"`
	Retention           json.RawMessage         `json:"retention"`
	UpdatedAt           json.RawMessage         `json:"updated_at"`
}

func (p *Plugin) loadSettings() (ledgerSettings, error) {
	settings := defaultSettings()
	errRead := p.withReader(func(ctx context.Context, r *storeReader) error {
		value, ok, errMeta := r.meta(ctx, metaKeySettings)
		if errMeta != nil || !ok {
			return errMeta
		}
		var stored ledgerSettings
		if json.Unmarshal([]byte(value), &stored) != nil {
			return nil // unreadable settings fall back to defaults
		}
		if stored.PlanOverrides != nil {
			settings.PlanOverrides = stored.PlanOverrides
		}
		if stored.ModelPriceOverrides != nil {
			settings.ModelPriceOverrides = stored.ModelPriceOverrides
		}
		if stored.DefaultCurrency != "" {
			settings.DefaultCurrency = stored.DefaultCurrency
		}
		settings.UpdatedAt = stored.UpdatedAt
		return nil
	})
	return settings, errRead
}

func (p *Plugin) settingsResponse(settings ledgerSettings) settingsResponse {
	cfg := p.Config()
	return settingsResponse{
		Schema:         routeSchema,
		ledgerSettings: settings,
		Retention:      statusRetention{MinuteDays: cfg.MinuteRetentionDays, HourDays: cfg.HourRetentionDays},
	}
}

func (p *Plugin) handleGetSettings(managementRequest) managementResponse {
	settings, errLoad := p.loadSettings()
	if errLoad != nil {
		return readError(errLoad)
	}
	return jsonResponse(http.StatusOK, p.settingsResponse(settings))
}

func (p *Plugin) handlePutSettings(req managementRequest) managementResponse {
	if len(req.Body) > maxBodyBytes {
		return errorResponse(badRequest("body_too_large", "the request body is limited to 256 KB"))
	}
	var body settingsPutBody
	if errDecode := decodeStrict(req.Body, &body); errDecode != nil {
		return errorResponse(badRequest("invalid_body", "invalid JSON body: "+errDecode.Error()))
	}
	settings, errValidate := validateSettings(body)
	if errValidate != nil {
		return errorResponse(errValidate)
	}
	updated := p.now().UTC().Format(time.RFC3339)
	settings.UpdatedAt = &updated
	raw, errMarshal := json.Marshal(settings)
	if errMarshal != nil {
		return readError(errMarshal)
	}
	// Written on the worker's connection under storeMu, like the batch
	// writes; the worker is still the only goroutine that swaps the store.
	p.storeMu.Lock()
	errWrite := errNoDatabase
	if p.store != nil {
		errWrite = p.store.SetMeta(metaKeySettings, string(raw))
	}
	p.storeMu.Unlock()
	if errWrite != nil {
		return readError(errWrite)
	}
	return jsonResponse(http.StatusOK, p.settingsResponse(settings))
}

func invalidSettings(message string) *apiError {
	return badRequest("invalid_settings", message)
}

func validateSettings(body settingsPutBody) (ledgerSettings, *apiError) {
	settings := defaultSettings()
	if body.DefaultCurrency != nil {
		currency := strings.TrimSpace(*body.DefaultCurrency)
		if !currencyPattern.MatchString(currency) {
			return ledgerSettings{}, invalidSettings("default_currency must be a three-letter ISO 4217 code such as USD")
		}
		settings.DefaultCurrency = currency
	}
	if len(body.PlanOverrides)+len(body.ModelPriceOverrides) > maxSettingsEntries {
		return ledgerSettings{}, invalidSettings("at most 2000 overrides in total")
	}
	for authIndex, override := range body.PlanOverrides {
		key := strings.TrimSpace(authIndex)
		if key == "" || len(key) > maxSettingsKeyLength {
			return ledgerSettings{}, invalidSettings("plan_overrides keys must be auth_index values of 1-256 characters")
		}
		override.Provider = strings.TrimSpace(override.Provider)
		override.Plan = strings.TrimSpace(override.Plan)
		override.Currency = strings.TrimSpace(override.Currency)
		where := "plan_overrides[" + quote(key) + "]"
		if override.Provider == "" || len(override.Provider) > maxSettingsNameLength {
			return ledgerSettings{}, invalidSettings(where + ".provider must be 1-64 characters")
		}
		if override.Plan == "" || len(override.Plan) > maxSettingsNameLength {
			return ledgerSettings{}, invalidSettings(where + ".plan must be 1-64 characters")
		}
		if math.IsNaN(override.MonthlyPrice) || override.MonthlyPrice < 0 || override.MonthlyPrice > maxMonthlyPrice {
			return ledgerSettings{}, invalidSettings(where + ".monthly_price must be between 0 and 1000000")
		}
		if override.Currency == "" {
			override.Currency = settings.DefaultCurrency
		}
		if !currencyPattern.MatchString(override.Currency) {
			return ledgerSettings{}, invalidSettings(where + ".currency must be a three-letter ISO 4217 code")
		}
		settings.PlanOverrides[key] = override
	}
	for model, prices := range body.ModelPriceOverrides {
		key := strings.TrimSpace(model)
		where := "model_price_overrides[" + quote(key) + "]"
		if key == "" || len(key) > maxSettingsKeyLength {
			return ledgerSettings{}, invalidSettings("model_price_overrides keys must be model names of 1-256 characters")
		}
		if prices.empty() {
			return ledgerSettings{}, invalidSettings(where + " must set at least one of input, output, cache_read, cache_write")
		}
		for _, v := range []*float64{prices.Input, prices.Output, prices.CacheRead, prices.CacheWrite} {
			if v != nil && (math.IsNaN(*v) || *v < 0 || *v > maxPerTokenPrice) {
				return ledgerSettings{}, invalidSettings(where + " prices are USD per token and must be between 0 and 1")
			}
		}
		settings.ModelPriceOverrides[key] = prices
	}
	return settings, nil
}
