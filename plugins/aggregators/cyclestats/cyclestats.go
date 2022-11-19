package cyclestats

import (
	_ "embed"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

var sampleConfig = `
  ## Available tags to attach to metrics:
  ## * id
  ## * cycle
  ## * device_config
  ## * grind_cycle
  ## * steam_cycle
  ## * waste_type
  ## * type
  ## * start_time
  ## * end_time
  ## * completed
  ## * successful
  portal_tags = [ "id", "grind_cycle", "steam_cycle" ]
`

type CycleStats struct {
    // caches for metric fields, names, and tags
    fieldCache map[uint64]map[string]float64
    nameCache  map[uint64]string
    tagCache   map[uint64]map[string]string
}

func NewCycleStats() telegraf.Aggregator {
    m := &CycleStats{}
    m.Reset()
    return m
}

func (*CycleStats) SampleConfig() string {
    return sampleConfig
}

func (m *CycleStats) Init() error {
    return nil
}

func (m *CycleStats) Add(in telegraf.Metric) {
    id := in.HashID()
    if _, ok := m.nameCache[id]; !ok {
        // hit an uncached metric, create caches for first time:
        m.nameCache[id] = in.Name()
        m.tagCache[id] = in.Tags()
        m.fieldCache[id] = make(map[string]float64)
        for k, v := range in.Fields() {
            if fv, ok := convert(v); ok {
                m.fieldCache[id][k] = fv
            }
        }
    } else {
        for k, v := range in.Fields() {
            if fv, ok := convert(v); ok {
                if _, ok := m.fieldCache[id][k]; !ok {
                    // hit an uncached field of a cached metric
                    m.fieldCache[id][k] = fv
                    continue
                }
                if fv < m.fieldCache[id][k] {
                    // set new minimum
                    m.fieldCache[id][k] = fv
                }
            }
        }
    }
}

func (m *CycleStats) Push(acc telegraf.Accumulator) {
    for id, _ := range m.nameCache {
        fields := map[string]interface{}{}
        for k, v := range m.fieldCache[id] {
            fields[k+"_min"] = v
        }
        acc.AddFields(m.nameCache[id], fields, m.tagCache[id])
    }
}

func (m *CycleStats) Reset() {
    m.fieldCache = make(map[uint64]map[string]float64)
    m.nameCache = make(map[uint64]string)
    m.tagCache = make(map[uint64]map[string]string)
}

func convert(in interface{}) (float64, bool) {
    switch v := in.(type) {
    case float64:
        return v, true
    case int64:
        return float64(v), true
    default:
        return 0, false
    }
}

func init() {
    aggregators.Add("min", func() telegraf.Aggregator {
        return NewCycleStats()
    })
}
