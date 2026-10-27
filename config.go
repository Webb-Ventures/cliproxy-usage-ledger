package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultDatabaseFileName    = "usage-ledger.db"
	defaultMinuteRetentionDays = 14
	defaultHourRetentionDays   = 400
	defaultPriceSourceURL      = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
	defaultPriceRefreshHours   = 24
)

// Config is the effective plugin configuration taken from
// plugins.configs.usage-ledger in the CLIProxyAPI config file.
type Config struct {
	// DatabasePath is the configured database path. Empty means
	// "<host working directory>/usage-ledger.db"; see ResolveDatabasePath.
	DatabasePath        string
	MinuteRetentionDays int
	HourRetentionDays   int
	// PriceSourceURL is the LiteLLM price JSON to fetch. Empty disables
	// fetching.
	PriceSourceURL    string
	PriceRefreshHours int
}

// DefaultConfig returns the configuration used when a key is omitted.
func DefaultConfig() Config {
	return Config{
		DatabasePath:        "",
		MinuteRetentionDays: defaultMinuteRetentionDays,
		HourRetentionDays:   defaultHourRetentionDays,
		PriceSourceURL:      defaultPriceSourceURL,
		PriceRefreshHours:   defaultPriceRefreshHours,
	}
}

// rawConfig mirrors the YAML mapping the host delivers in config_yaml. The host
// also injects "enabled" and "priority" (and may pass through other keys such
// as "store"); unknown keys are ignored.
type rawConfig struct {
	DatabasePath        *string `yaml:"database-path"`
	MinuteRetentionDays *int    `yaml:"minute-retention-days"`
	HourRetentionDays   *int    `yaml:"hour-retention-days"`
	PriceSourceURL      *string `yaml:"price-source-url"`
	PriceRefreshHours   *int    `yaml:"price-refresh-hours"`
}

// ParseConfig decodes the plugin's config_yaml payload, applying defaults for
// omitted keys and rejecting values that cannot be used.
func ParseConfig(configYAML []byte) (Config, error) {
	cfg := DefaultConfig()
	if len(strings.TrimSpace(string(configYAML))) == 0 {
		return cfg, nil
	}
	var raw rawConfig
	if errUnmarshal := yaml.Unmarshal(configYAML, &raw); errUnmarshal != nil {
		return Config{}, fmt.Errorf("usage-ledger: invalid plugin config: %w", errUnmarshal)
	}
	if raw.DatabasePath != nil {
		cfg.DatabasePath = strings.TrimSpace(*raw.DatabasePath)
	}
	if raw.MinuteRetentionDays != nil {
		cfg.MinuteRetentionDays = *raw.MinuteRetentionDays
	}
	if raw.HourRetentionDays != nil {
		cfg.HourRetentionDays = *raw.HourRetentionDays
	}
	if raw.PriceSourceURL != nil {
		// An explicit empty value disables price fetching; omitting the key
		// keeps the default URL.
		cfg.PriceSourceURL = strings.TrimSpace(*raw.PriceSourceURL)
	}
	if raw.PriceRefreshHours != nil {
		cfg.PriceRefreshHours = *raw.PriceRefreshHours
	}
	if errValidate := cfg.validate(); errValidate != nil {
		return Config{}, errValidate
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.MinuteRetentionDays <= 0 {
		return fmt.Errorf("usage-ledger: minute-retention-days must be positive, got %d", c.MinuteRetentionDays)
	}
	if c.HourRetentionDays <= 0 {
		return fmt.Errorf("usage-ledger: hour-retention-days must be positive, got %d", c.HourRetentionDays)
	}
	if c.PriceRefreshHours <= 0 {
		return fmt.Errorf("usage-ledger: price-refresh-hours must be positive, got %d", c.PriceRefreshHours)
	}
	if c.PriceSourceURL == "" {
		return nil // fetching disabled
	}
	parsed, errParse := url.Parse(c.PriceSourceURL)
	if errParse != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("usage-ledger: price-source-url must be an absolute http(s) URL, got %q", c.PriceSourceURL)
	}
	return nil
}

// ResolveDatabasePath returns the absolute database path. The host does not
// tell plugins where its plugins directory is (config_yaml only carries the
// plugins.configs.<id> mapping) and loads the library from a shadow copy in the
// OS temp dir, so relative paths and the empty default resolve against the
// host process working directory. A leading "~" expands to the home directory.
func (c Config) ResolveDatabasePath(workingDir string) (string, error) {
	path := strings.TrimSpace(c.DatabasePath)
	if path == "" {
		path = defaultDatabaseFileName
	}
	if path == "~" || strings.HasPrefix(path, "~/") || strings.HasPrefix(path, `~\`) {
		home, errHome := os.UserHomeDir()
		if errHome != nil {
			return "", fmt.Errorf("usage-ledger: resolve database-path: %w", errHome)
		}
		path = filepath.Join(home, path[1:])
	}
	if !filepath.IsAbs(path) {
		if strings.TrimSpace(workingDir) == "" {
			return "", fmt.Errorf("usage-ledger: cannot resolve relative database-path %q without a working directory", path)
		}
		path = filepath.Join(workingDir, path)
	}
	return filepath.Clean(path), nil
}
