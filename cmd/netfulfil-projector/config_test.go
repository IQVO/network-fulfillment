package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// TestLoadProjectorConfig: every missing or blank mandatory value is a boot
// error naming the variable; there is no localhost broker default.
func TestLoadProjectorConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr error
		wantVar string
		want    projectorConfig
	}{
		{name: "nothing set", env: map[string]string{}, wantErr: errMissingAnalyticsURL, wantVar: "ANALYTICS_DATABASE_URL"},
		{name: "blank url", env: map[string]string{"ANALYTICS_DATABASE_URL": "  ", "KAFKA_BROKERS": "k:9092"}, wantErr: errMissingAnalyticsURL, wantVar: "ANALYTICS_DATABASE_URL"},
		{name: "no brokers", env: map[string]string{"ANALYTICS_DATABASE_URL": "postgres://a"}, wantErr: errMissingKafkaBrokers, wantVar: "KAFKA_BROKERS"},
		{name: "only separators", env: map[string]string{"ANALYTICS_DATABASE_URL": "postgres://a", "KAFKA_BROKERS": " , ,"}, wantErr: errMissingKafkaBrokers, wantVar: "KAFKA_BROKERS"},
		{
			name: "both set",
			env:  map[string]string{"ANALYTICS_DATABASE_URL": " postgres://a ", "KAFKA_BROKERS": "k1:9092, k2:9092,"},
			want: projectorConfig{analyticsURL: "postgres://a", kafkaBrokers: []string{"k1:9092", "k2:9092"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadProjectorConfig(envMap(tc.env))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantVar) {
					t.Fatalf("error %q must name %s", err, tc.wantVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
