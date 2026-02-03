package output

import (
	"encoding/json"
	"io"
)

type EventOutput struct {
	Domain      string `json:"domain"`
	LogURL      string `json:"source_log"`
	ObservedAt  string `json:"observed_at"`
	SCTimestamp string `json:"sct_timestamp,omitempty"`
	LatencyMS   int64  `json:"latency_ms,omitempty"`
}

func NewJSONEncoder(w io.Writer) *json.Encoder {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc
}
