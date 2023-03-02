package cyclestats

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/processors"
)

var sampleConfig = ``

type CycleStats struct {
	Name    string          `toml:"name"`
	GroupBy []string        `toml:"group_by"`
	Log     telegraf.Logger `toml:"-"`
	Fields  map[string][]string

	cache   map[string][]telegraf.Metric
	filters filter.Filter
}

func (r *CycleStats) Description() string {
	return "Aggregates cycle stats"
}

func New() *CycleStats {
	// Create object
	cyclestats := CycleStats{}

	// Setup defaults
	cyclestats.Fields = make(map[string][]string)

	cyclestats.Fields["steam_params"] = []string{
		"steam_type",
		"cook_temp",
		"control_temp",
		"hot_drain_temp",
		"pv_unsafe",
		"pv_too_low",
		"drain_open_duration",
		"drain_to_sec1",
		"drain_to_sec2",
		"wait_pressure",
	}

	cyclestats.Fields["steam_stats"] = []string{
		"error",
		"flows",
		"pd_timeouts",
		"stag_recoveries",
		"stop_cook_count",
	}

	cyclestats.Fields["vessel_status"] = []string{
		"lid_position",
		"shroud_inside_up",
		"shroud_inside_down",
		"shroud_outside_up",
		"shroud_outside_down",
		"shrouds",
		"vessel_temperature",
		"heater_temperature",
		"runaway_temperature",
		"pv_sensor_type",
		"vessel_pressure",
		"seal_pressure",
		"accumulator_pressure",
		"top_cover",
		"top_lid_open",
		"top_lid_closed",
		"bottom_lid_open",
		"bottom_lid_closed",
	}

	cyclestats.Fields["steam_stats"] = []string{
		"error",
		"flows",
		"pd_timeouts",
		"stag_recoveries",
		"stop_cook_count",
	}

	cyclestats.Fields["system_status"] = []string{
		"cover_interlock",
		"battery_fault",
		"line_current",
		"deodorizer_tank",
		"oil_tank",
		"flow_count",
		"bag_door",
		"top_cover",
		"reservoir",
		"fans",
	}

	cyclestats.Fields["sys_status_mngr"] = []string{
		"heater",
		"vacuum",
		"water",
		"compressor",
	}

	cyclestats.Fields["grinder"] = []string{
		"grinder_state",
		"jack_status",
		"switches_bottom",
		"switches_top",
		"reversals",
	}

	cyclestats.Fields["vessel_lid_failure"] = []string{
		"top_lid_open_failed",
		"top_lid_close_failed",
		"bottom_lid_open_failed",
		"bottom_lid_close_failed",
		"inside_shroud_open_failed",
		"inside_shroud_close_failed",
		"accumulator_not_pressurized",
		"seals_vacuum_failed",
		"jack_up_failed",
		"close_seals_failed",
		"vent_seals_failed",
		"compressor_throttled",
		"pv_mismatch",
		"error",
	}

	cyclestats.GroupBy = []string{"*"}

	// Initialize cache
	cyclestats.Reset()

	return &cyclestats
}

func (*CycleStats) SampleConfig() string {
	return sampleConfig
}

func (t *CycleStats) Init() error {
	t.Log.Info("Initializing Portal CycleStats Processor")
	return nil
}


func (t *CycleStats) Reset() {
	t.cache = make(map[string][]telegraf.Metric)
}

func (t *CycleStats) generateGroupByKey(m telegraf.Metric) (string, error) {
	// Create the filter.Filter objects if they have not been created
	if t.filters == nil && len(t.GroupBy) > 0 {
		var err error
		t.filters, err = filter.Compile(t.GroupBy)
		if err != nil {
			return "", fmt.Errorf("could not compile pattern: %v %v", t.GroupBy, err)
		}
	}

	groupkey := m.Name() + "&" + m.Time().Truncate(1000*time.Millisecond).String()

	return groupkey, nil
}

func (t *CycleStats) groupBy(m telegraf.Metric) {
	// Generate the metric group key
	groupkey, err := t.generateGroupByKey(m)
	if err != nil {
		// If we could not generate the groupkey, fail hard
		// by dropping this and all subsequent metrics
		t.Log.Errorf("Could not generate group key: %v", err)
		return
	}

	// Initialize the key with an empty list if necessary
	if _, ok := t.cache[groupkey]; !ok {
		t.cache[groupkey] = make([]telegraf.Metric, 0, 10)
	}

	// Append the metric to the corresponding key list
	t.cache[groupkey] = append(t.cache[groupkey], m)
}

func (t *CycleStats) Apply(in ...telegraf.Metric) []telegraf.Metric {

	groupkey := ""
	// Add the metrics received to our internal cache
	var measurment string
	for _, m := range in {
		measurment = m.Name()
		// When tracking metrics this plugin could deadlock the input by
		// holding undelivered metrics while the input waits for metrics to be
		// delivered.  Instead, treat all handled metrics as delivered and
		// produced metrics as untracked in a similar way to aggregators.
		m.Drop()
		gkey, _ := t.generateGroupByKey(m)
		groupkey = gkey
		// Check if the metric has any of the fields over which we are aggregating
		hasField := false
		for _, f := range t.Fields[m.Name()] {
			if m.HasField(f) {
				hasField = true
				break
			}
		}
		if !hasField {
			continue
		}

		// Add the metric to the internal cache
		t.groupBy(m)
	}

	if keyCount := len(t.cache[groupkey]); keyCount >= len(t.Fields[measurment]) {
		return t.push()
	}

	return []telegraf.Metric{}
}

func (t *CycleStats) push() []telegraf.Metric {
	// Generate aggregations list using the selected fields
	aggs := make([]telegraf.Metric, 0)
	for _, ms := range t.cache {
		aggregate, _ := t.Aggregate(ms)
		aggs = append(aggs, aggregate)
	}

	t.Reset()

	return aggs
}

func (c *CycleStats) Aggregate(ms []telegraf.Metric) (telegraf.Metric, error) {
	var metric telegraf.Metric
	for _, m := range ms {
		if metric == nil {
			metric = m.Copy()
		} else {
			for _, field := range m.FieldList() {
				metric.AddField(field.Key, field.Value)
			}
		}
	}
	return metric, nil
}

func init() {
	processors.Add("cyclestats", func() telegraf.Processor {
		return New()
	})
}
