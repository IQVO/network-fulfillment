package retailnetwork

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

func weekdays(days ...time.Weekday) []time.Weekday { return days }

func sampleProfile() contract.NodeProfile {
	return contract.NodeProfile{
		SiteId:   "FC-1",
		Timezone: "America/Sao_Paulo",
		Cutoffs: []contract.ProfileCutoff{
			{LocalTime: "18:00", Days: weekdays(time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday), MaxOrders: 120},
			{LocalTime: "10:30", Days: weekdays(time.Saturday)},
		},
		HandlingTime:     47*time.Minute + 30*time.Second,
		ThroughputPerDay: 600,
		Fingerprint:      "fp-1",
	}
}

type decodedProfileBody struct {
	VendorID            string `json:"vendorId"`
	ProfileRevision     int    `json:"profileRevision"`
	Timezone            string `json:"timezone"`
	OperatingHoursByDay map[string]struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"operatingHoursByDay"`
	HandlingTimeMinutes int `json:"handlingTimeMinutes"`
	Cutoffs             []struct {
		LocalTime string   `json:"localTime"`
		MaxOrders *int     `json:"maxOrders"`
		Days      []string `json:"days"`
	} `json:"cutoffs"`
	ThroughputCap struct {
		Value    int    `json:"value"`
		TimeUnit string `json:"timeUnit"`
		Unit     string `json:"unit"`
	} `json:"throughputCap"`
}

func TestBuildNodeProfileBody_SaysWhatTheProfileSaysAndNothingElse(t *testing.T) {
	body, err := buildNodeProfileBody("V-WS", 3, sampleProfile())
	if err != nil {
		t.Fatalf("buildNodeProfileBody: %v", err)
	}
	var got decodedProfileBody
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}

	if got.VendorID != "V-WS" || got.ProfileRevision != 3 || got.Timezone != "America/Sao_Paulo" {
		t.Errorf("identity = %q rev %d %q, want V-WS rev 3 America/Sao_Paulo", got.VendorID, got.ProfileRevision, got.Timezone)
	}
	if got.HandlingTimeMinutes != 48 {
		t.Errorf("handlingTimeMinutes = %d, want 47m30s rounded UP to 48 (never promise less time than the p95 needs)", got.HandlingTimeMinutes)
	}
	if got.ThroughputCap.Value != 600 || got.ThroughputCap.TimeUnit != "DAY" || got.ThroughputCap.Unit != "ORDER" {
		t.Errorf("throughputCap = %+v, want 600 ORDER per DAY", got.ThroughputCap)
	}
	if len(got.Cutoffs) != 2 {
		t.Fatalf("cutoffs = %d, want 2", len(got.Cutoffs))
	}
	weekday, saturday := got.Cutoffs[0], got.Cutoffs[1]
	if weekday.LocalTime != "18:00" || weekday.MaxOrders == nil || *weekday.MaxOrders != 120 ||
		len(weekday.Days) != 5 || weekday.Days[0] != "monday" || weekday.Days[4] != "friday" {
		t.Errorf("weekday cutoff = %+v, want 18:00, 120 orders, monday..friday", weekday)
	}
	if saturday.LocalTime != "10:30" || saturday.MaxOrders != nil || len(saturday.Days) != 1 || saturday.Days[0] != "saturday" {
		t.Errorf("saturday cutoff = %+v, want 10:30 on saturday with maxOrders OMITTED (unknown is not zero)", saturday)
	}

	var raw map[string]json.RawMessage
	_ = json.Unmarshal(body, &raw)
	for _, key := range []string{"holidays", "sortDwellMinutes"} {
		if _, sent := raw[key]; sent {
			t.Errorf("the body sends %q: the fleet holds no such fact (ADR 0027 Decision 3)", key)
		}
	}
}

func TestBuildNodeProfileBody_OperatingHoursAreTheDaysWithACutoffAndEveryCutoffFitsInside(t *testing.T) {
	body, err := buildNodeProfileBody("V-WS", 1, sampleProfile())
	if err != nil {
		t.Fatalf("buildNodeProfileBody: %v", err)
	}
	var got decodedProfileBody
	_ = json.Unmarshal(body, &got)

	if len(got.OperatingHoursByDay) != 6 {
		t.Fatalf("operating days = %v, want the 6 days that have a cutoff (sunday has none, so it is closed)", got.OperatingHoursByDay)
	}
	if _, open := got.OperatingHoursByDay["sunday"]; open {
		t.Error("sunday has no cutoff and must not be declared open")
	}
	// node-topology refuses a cutoff outside the hours of any of its days (end inclusive).
	for _, c := range got.Cutoffs {
		for _, day := range c.Days {
			hours, open := got.OperatingHoursByDay[day]
			if !open || c.LocalTime < hours.Start || c.LocalTime > hours.End {
				t.Errorf("cutoff %s on %s is outside the declared hours %+v", c.LocalTime, day, hours)
			}
			if hours.Start >= hours.End {
				t.Errorf("hours of %s = %+v, want opening before closing", day, hours)
			}
		}
	}
}

func TestBuildNodeProfileBody_IsDeterministicByteForByte(t *testing.T) {
	first, err := buildNodeProfileBody("V-WS", 2, sampleProfile())
	if err != nil {
		t.Fatalf("buildNodeProfileBody: %v", err)
	}
	for range 20 {
		again, _ := buildNodeProfileBody("V-WS", 2, sampleProfile())
		if string(again) != string(first) {
			t.Fatalf("two builds differ:\n%s\n%s", first, again)
		}
	}
}

func TestBuildNodeProfileBody_RefusesAProfileTheNetworkCouldNotTake(t *testing.T) {
	cases := map[string]struct {
		revision int
		mutate   func(*contract.NodeProfile)
	}{
		"revision 0":               {0, func(*contract.NodeProfile) {}},
		"no timezone":              {1, func(p *contract.NodeProfile) { p.Timezone = "" }},
		"no cutoffs":               {1, func(p *contract.NodeProfile) { p.Cutoffs = nil }},
		"a cutoff time not HH:mm":  {1, func(p *contract.NodeProfile) { p.Cutoffs[0].LocalTime = "6pm" }},
		"an hour of 24":            {1, func(p *contract.NodeProfile) { p.Cutoffs[0].LocalTime = "24:00" }},
		"a cutoff with no days":    {1, func(p *contract.NodeProfile) { p.Cutoffs[0].Days = nil }},
		"an unknown weekday":       {1, func(p *contract.NodeProfile) { p.Cutoffs[0].Days = []time.Weekday{time.Weekday(9)} }},
		"a negative ceiling":       {1, func(p *contract.NodeProfile) { p.Cutoffs[0].MaxOrders = -1 }},
		"no handling time":         {1, func(p *contract.NodeProfile) { p.HandlingTime = 0 }},
		"a negative handling time": {1, func(p *contract.NodeProfile) { p.HandlingTime = -time.Minute }},
		"no throughput":            {1, func(p *contract.NodeProfile) { p.ThroughputPerDay = 0 }},
		"two cutoffs at one time on a shared day": {1, func(p *contract.NodeProfile) {
			p.Cutoffs = append(p.Cutoffs, contract.ProfileCutoff{LocalTime: "18:00", Days: weekdays(time.Friday)})
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p := sampleProfile()
			tc.mutate(&p)
			if _, err := buildNodeProfileBody("V-WS", tc.revision, p); !errors.Is(err, ErrBadProfile) {
				t.Fatalf("err = %v, want ErrBadProfile", err)
			}
		})
	}
}

func TestBuildNodeProfileBody_AcceptsTheEdgesTheNetworkAccepts(t *testing.T) {
	p := sampleProfile()
	p.Cutoffs = []contract.ProfileCutoff{
		{LocalTime: "00:00", Days: weekdays(time.Sunday)},
		{LocalTime: "23:59", Days: weekdays(time.Sunday), MaxOrders: 1},
	}
	p.HandlingTime = time.Second // rounds up to the one minute the network requires
	body, err := buildNodeProfileBody("V-WS", 1, p)
	if err != nil {
		t.Fatalf("buildNodeProfileBody: %v", err)
	}
	var got decodedProfileBody
	_ = json.Unmarshal(body, &got)
	if got.HandlingTimeMinutes != 1 {
		t.Errorf("handlingTimeMinutes = %d, want 1", got.HandlingTimeMinutes)
	}
	if hours := got.OperatingHoursByDay["sunday"]; hours.Start != "00:00" || hours.End != "23:59" {
		t.Errorf("sunday = %+v, want 00:00 to 23:59, which holds a cutoff at 23:59 (end is inclusive)", hours)
	}
}
