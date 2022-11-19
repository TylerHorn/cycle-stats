package cyclestats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/processors"
)

type MetadataProcessor struct {
	PortalTags          []string        `toml:"portal_tags"`
	Timeout          config.Duration `toml:"timeout"`
	Log              telegraf.Logger `toml:"-"`
	ApiKey			 string `toml:"api_key"`
	ApiUrl			 string `toml:"api_url"`
	metaCache		 map[string]int
}


type Response struct {
	Id			  	int `json:"id"`
	Cycle         	int `json:"cycle"`
	DeviceConfig   	int `json:"device_config"`
	GrindCycle		int `json:"grind_cycle"`
	SteamCycle      int `json:"steam_cycle"`
	WasteType      	string `json:"waste_type"`
	Type        	string `json:"type"`
	StartTime       string `json:"start_time"`
	EndTime      	string `json:"end_time"`
	Completed     	bool `json:"completed"`
	Successful    	bool `json:"successful"`
}

type Body struct {
	Timestamp string `json:"timestamp"`
}

var meta_resp Response

const sampleConfig = `
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

const (
	DefaultTimeout             = 10 * time.Second
)

func (r *MetadataProcessor) SampleConfig() string {
	return sampleConfig
}

func (r *MetadataProcessor) Description() string {
	return "Attach Portal metadata to metrics"
}

func (r *MetadataProcessor) Apply(in ...telegraf.Metric) []telegraf.Metric {
	// Add metadata info for each metric
	for _, metric := range in {
		metric_type := metric.Name()
		if (metric_type == "state_change") {
			r.clearMetaCache()
			metric.Drop()
		}

		if _, ok := r.metaCache["metadata"]; !ok {
			tm := metric.Time().In(time.UTC)
			if resp, err := r.getMetadata(tm, r.ApiKey, r.ApiUrl); err == nil {
				meta_resp = resp
			}
		}

		type TagType struct {
			Tag			string
			MetricType string
		}

		tagtype := TagType{MetricType: metric_type}

		for _, tag := range r.PortalTags {
			tagtype.Tag = tag
			if v := r.metaCache[tag]; v != 0 {
				switch tagtype {
				case TagType{"steam_cycle", "steam"}:
					metric.AddField(tag, v)
				case TagType{"metadata", "steam"}:
					metric.AddTag(tag, fmt.Sprint(v))
				case TagType{"grind_cycle", "grind"}:
					metric.AddField(tag, v)
				case TagType{"metadata", "grind"}:
					metric.AddTag(tag, fmt.Sprint(v))
				case TagType{"metadata", "notification"}:
					metric.AddField(tag, v)
				}

				// if (metric_type == "steam" && tag == "steam_cycle") {
				// 	r.Log.Debugf("Adding field: %s with value: %d", tag, v)
				// 	metric.AddField(tag, v)
				// }
				// if (metric_type == "grind" && tag == "grind_cycle") {
				// 	r.Log.Debugf("Adding field: %s with value: %d", tag, v)
				// 	metric.AddField(tag, v)
				// }
				// if (metric_type == "notification" && tag == "metadata" ) {
				// 	r.Log.Debugf("Adding field: %s with value: %d", tag, v)
				// 	metric.AddField(tag, v)
				// }
				// metric.AddTag(tag, fmt.Sprint(v))
			}
		}
	}
	return in
}

func (r *MetadataProcessor) Init() error {
	r.Log.Info("Initializing Portal Metadata Processor")
	if len(r.PortalTags) == 0 {
		return errors.New("no tags specified in configuration")
	}
	return nil
}

func init() {
	processors.Add("metadata", func() telegraf.Processor {
		return &MetadataProcessor{
			metaCache: make(map[string]int),
		}
	})
}

func (r *MetadataProcessor) getMetadata(tm time.Time, apiKey string, apiUrl string) (Response, error) {
	var token = fmt.Sprintf("TOKEN %s", apiKey)

	jsonBody := []byte(fmt.Sprintf(`{"timestamp": "%s"}`, tm.Format("2006-01-02T15:04:05.000Z")))
	bodyReader := bytes.NewReader(jsonBody)

	client := http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest(http.MethodPost, apiUrl, bodyReader)
	if err != nil {
		fmt.Println("Error creating request")
	}
	req.Header.Add("Authorization", token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("error during request: %w", err)
	}
    if resp.StatusCode >= 305 || resp.StatusCode <= 200 {
        return Response{}, fmt.Errorf("unsuccessful status code: %s", resp.Status)
    }


	body, err := io.ReadAll(resp.Body) // response body is []byte
	if err != nil {
		return Response{}, fmt.Errorf("error while reading response body: %w", err)
	}

	defer resp.Body.Close()
	var result Response

	if err := json.Unmarshal(body, &result); err != nil {   // Parse []byte to go struct pointer
		return Response{}, fmt.Errorf("error during request: %w", err)
	}
	r.setMetaCache(result)
	return result, nil
}

func (r *MetadataProcessor) setMetaCache(resp Response) {
	r.metaCache["metadata"] = resp.Id
	r.metaCache["steam_cycle"] = resp.SteamCycle
	r.metaCache["grind_cycle"] = resp.GrindCycle
}

func (r *MetadataProcessor) clearMetaCache() {
	r.metaCache = make(map[string]int)
}

// PrettyPrint to print struct in a readable way
func PrettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}
