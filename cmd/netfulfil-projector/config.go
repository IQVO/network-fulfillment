package main

// Boot configuration of the projector: both real dependencies are mandatory
// and a missing one is an error that names the variable (the real-wiring
// ADR). There is no localhost default for the broker list.

import (
	"errors"
	"strings"
)

// errMissingKafkaBrokers is returned when KAFKA_BROKERS is empty: the
// projector consumes the analytics topic and has no other input.
var errMissingKafkaBrokers = errors.New("KAFKA_BROKERS is required: the projector consumes the analytics topic from a real Kafka (comma-separated host:port list)")

// projectorConfig is the validated mandatory configuration.
type projectorConfig struct {
	analyticsURL string
	kafkaBrokers []string
}

// loadProjectorConfig reads ANALYTICS_DATABASE_URL and KAFKA_BROKERS through
// get (os.Getenv in production, a map lookup in tests).
func loadProjectorConfig(get func(string) string) (projectorConfig, error) {
	url := strings.TrimSpace(get("ANALYTICS_DATABASE_URL"))
	if url == "" {
		return projectorConfig{}, errMissingAnalyticsURL
	}
	var brokers []string
	for _, b := range strings.Split(get("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return projectorConfig{}, errMissingKafkaBrokers
	}
	return projectorConfig{analyticsURL: url, kafkaBrokers: brokers}, nil
}
