package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/taubyte/go-sdk/database"
	baseEvent "github.com/taubyte/go-sdk/event"
	httpEvent "github.com/taubyte/go-sdk/http/event"
)

const (
	dbName    = "seguentedb"
	metricKey = "metric"
	metricMin = 0.0
	metricMax = 100.0
)

var errValueNotFound = errors.New("value not found")

type valueAddress struct {
	IP       string  `json:"ip"`
	Port     *string `json:"port"`
	Protocol *string `json:"protocol"`
}

type valuePayload struct {
	PeerID  string                  `json:"peerId"`
	Address valueAddress            `json:"address"`
	Values  map[string]valueMetrics `json:"values"`
	Raw     string                  `json:"raw"`
}

type valueMetrics struct {
	Current   float64 `json:"current"`
	SoftLimit float64 `json:"softLimit"`
	HardLimit float64 `json:"hardLimit"`
}

type legacyValueLimits struct {
	Soft float64 `json:"soft"`
	Hard float64 `json:"hard"`
}

// ---------- Utility Functions ----------

func setCORSHeaders(h httpEvent.Event) {
	h.Headers().Set("Access-Control-Allow-Origin", "*")
	h.Headers().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	h.Headers().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func handleHTTPError(h httpEvent.Event, err error, code int) uint32 {
	h.Write([]byte(err.Error()))
	h.Return(code)
	return 1
}

func sendJSONResponse(h httpEvent.Event, data interface{}) uint32 {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return handleHTTPError(h, err, 500)
	}
	h.Headers().Set("Content-Type", "application/json")
	h.Write(jsonData)
	h.Return(200)
	return 0
}

func openDB() (database.Database, error) {
	return database.New(dbName)
}

func validateValuePayload(v valuePayload) error {
	if strings.TrimSpace(v.PeerID) == "" {
		return fmt.Errorf("peerId is required")
	}
	if strings.TrimSpace(v.Address.IP) == "" {
		return fmt.Errorf("address.ip is required")
	}
	if strings.TrimSpace(v.Raw) == "" {
		return fmt.Errorf("raw is required")
	}
	if len(v.Values) == 0 {
		return fmt.Errorf("values.%s is required", metricKey)
	}

	metric, ok := v.Values[metricKey]
	if !ok {
		return fmt.Errorf("values.%s is required", metricKey)
	}

	if !isFinite(metric.Current) || !isFinite(metric.SoftLimit) || !isFinite(metric.HardLimit) {
		return fmt.Errorf("values.%s contains invalid numbers", metricKey)
	}
	if metric.SoftLimit < metricMin || metric.SoftLimit > metricMax {
		return fmt.Errorf("values.%s.softLimit must be between %.0f and %.0f", metricKey, metricMin, metricMax)
	}
	if metric.HardLimit < metricMin || metric.HardLimit > metricMax {
		return fmt.Errorf("values.%s.hardLimit must be between %.0f and %.0f", metricKey, metricMin, metricMax)
	}
	if metric.Current < metricMin || metric.Current > metricMax {
		return fmt.Errorf("values.%s.current must be between %.0f and %.0f", metricKey, metricMin, metricMax)
	}
	if metric.SoftLimit > metric.HardLimit {
		return fmt.Errorf("values.%s.softLimit must be <= hardLimit", metricKey)
	}
	return nil
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func migrateLegacyValues(data []byte) (map[string]valueMetrics, bool) {
	var legacy struct {
		Limits *legacyValueLimits `json:"limits"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil || legacy.Limits == nil {
		return nil, false
	}

	soft := clampMetricValue(legacy.Limits.Soft)
	hard := clampMetricValue(legacy.Limits.Hard)
	if soft > hard {
		soft = hard
	}

	return map[string]valueMetrics{
		metricKey: {
			Current:   soft,
			SoftLimit: soft,
			HardLimit: hard,
		},
	}, true
}

func clampMetricValue(value float64) float64 {
	if !isFinite(value) {
		return metricMin
	}
	if value < metricMin {
		return metricMin
	}
	if value > metricMax {
		return metricMax
	}
	return value
}

func normaliseValues(values map[string]valueMetrics) (map[string]valueMetrics, bool, error) {
	if values == nil {
		return nil, false, fmt.Errorf("values.%s is required", metricKey)
	}

	metric, ok := values[metricKey]
	if !ok {
		return nil, false, fmt.Errorf("values.%s is required", metricKey)
	}

	normalised := valueMetrics{
		Current:   clampMetricValue(metric.Current),
		SoftLimit: clampMetricValue(metric.SoftLimit),
		HardLimit: clampMetricValue(metric.HardLimit),
	}

	if normalised.SoftLimit > normalised.HardLimit {
		normalised.SoftLimit = normalised.HardLimit
	}
	if normalised.Current > normalised.HardLimit {
		normalised.Current = normalised.HardLimit
	}
	if normalised.Current < metricMin {
		normalised.Current = metricMin
	}

	changed := metric.Current != normalised.Current ||
		metric.SoftLimit != normalised.SoftLimit ||
		metric.HardLimit != normalised.HardLimit

	if changed {
		values[metricKey] = normalised
	}

	return values, changed, nil
}

func decodeValuePayload(data []byte, storageKey string) (valuePayload, bool, error) {
	var payload valuePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return payload, false, err
	}

	updated := false
	if len(payload.Values) == 0 {
		if migrated, ok := migrateLegacyValues(data); ok {
			payload.Values = migrated
			updated = true
		}
	}

	values, changed, err := normaliseValues(payload.Values)
	if err != nil {
		return payload, false, err
	}
	if changed {
		updated = true
	}
	payload.Values = values

	if strings.TrimSpace(payload.PeerID) == "" {
		trimmed := strings.TrimPrefix(storageKey, "/peer/")
		if trimmed == "" {
			trimmed = storageKey
		}
		payload.PeerID = trimmed
		updated = true
	}

	if err := validateValuePayload(payload); err != nil {
		return payload, false, err
	}

	return payload, updated, nil
}

func isPreflight(h httpEvent.Event) bool {
	if method, err := h.Method(); err == nil && method == "OPTIONS" {
		h.Return(204)
		return true
	}
	return false
}

func getPeerIDFromPath(h httpEvent.Event) (string, error) {
	path, err := h.Path()
	if err != nil {
		return "", fmt.Errorf("failed to read path")
	}

	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "", fmt.Errorf("missing peerId")
	}

	segments := strings.Split(trimmed, "/")
	id := segments[len(segments)-1]
	if id == "" {
		return "", fmt.Errorf("missing peerId")
	}

	return id, nil
}

func getPeerIDFromRequest(h httpEvent.Event) (string, error) {
	id, err := getPeerIDFromPath(h)
	if err == nil && strings.TrimSpace(id) != "delete" {
		return id, nil
	}

	query := h.Query()
	if queryPeerID, err := query.Get("peerId"); err == nil {
		if trimmed := strings.TrimSpace(queryPeerID); trimmed != "" {
			return trimmed, nil
		}
	}
	if queryID, err := query.Get("id"); err == nil {
		if trimmed := strings.TrimSpace(queryID); trimmed != "" {
			return trimmed, nil
		}
	}

	body, err := io.ReadAll(h.Body())
	if err != nil {
		return "", fmt.Errorf("failed to read request body")
	}
	if len(body) == 0 {
		return "", fmt.Errorf("missing peerId")
	}

	var payload struct {
		ID     string `json:"id"`
		PeerID string `json:"peerId"`
	}
	if err = json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("invalid payload format")
	}

	if trimmed := strings.TrimSpace(payload.PeerID); trimmed != "" {
		return trimmed, nil
	}

	if trimmed := strings.TrimSpace(payload.ID); trimmed != "" {
		return trimmed, nil
	}

	return "", fmt.Errorf("missing peerId")
}

func findValueByPeerID(db database.Database, peerID string) (string, []byte, error) {
	key := "/peer/" + peerID
	if data, err := db.Get(key); err == nil {
		return key, data, nil
	}

	return "", nil, errValueNotFound
}

// ---------- CRUD Handlers ----------

//export listValues
func listValues(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)
	if isPreflight(h) {
		return 0
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	keys, err := db.List("/peer")
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to list values"), 500)
	}

	values := make([]valuePayload, 0, len(keys))
	for _, key := range keys {
		id := key
		data, err := db.Get(id)
		if err != nil {
			return handleHTTPError(h, fmt.Errorf("failed to read value for key %s", id), 500)
		}

		payload, updated, err := decodeValuePayload(data, id)
		if err != nil {
			return handleHTTPError(h, fmt.Errorf("stored value for key %s is invalid: %w", id, err), 500)
		}

		if updated {
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return handleHTTPError(h, fmt.Errorf("failed to encode value for key %s", id), 500)
			}
			if err = db.Put(id, payloadJSON); err != nil {
				return handleHTTPError(h, fmt.Errorf("failed to update value for key %s", id), 500)
			}
		}

		values = append(values, payload)
	}

	return sendJSONResponse(h, map[string]interface{}{
		"count":  len(values),
		"values": values,
	})
}

//export registerValue
func registerValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)
	if isPreflight(h) {
		return 0
	}

	body, err := io.ReadAll(h.Body())
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to read request body"), 400)
	}

	var payload valuePayload
	if err = json.Unmarshal(body, &payload); err != nil {
		return handleHTTPError(h, fmt.Errorf("invalid payload format"), 400)
	}

	if len(payload.Values) == 0 {
		if migrated, ok := migrateLegacyValues(body); ok {
			payload.Values = migrated
		}
	}

	if err = validateValuePayload(payload); err != nil {
		return handleHTTPError(h, err, 400)
	}

	// Normalise persisted structure to a single metric entry.
	values, _, err := normaliseValues(payload.Values)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}
	metric := values[metricKey]
	payload.Values = map[string]valueMetrics{
		metricKey: metric,
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	peerID := strings.TrimSpace(payload.PeerID)
	payload.PeerID = peerID

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to encode payload"), 500)
	}

	key := "/peer/" + peerID
	if err = db.Put(key, payloadJSON); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to store value"), 500)
	}

	return sendJSONResponse(h, map[string]string{
		"peerId": peerID,
		"status": "created",
	})
}

//export getValue
func getValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)
	if isPreflight(h) {
		return 0
	}

	peerID, err := getPeerIDFromRequest(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	key, data, err := findValueByPeerID(db, peerID)
	if err != nil {
		if errors.Is(err, errValueNotFound) {
			return handleHTTPError(h, err, 404)
		}
		return handleHTTPError(h, fmt.Errorf("failed to read value"), 500)
	}

	payload, updated, err := decodeValuePayload(data, key)
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("stored value is invalid: %w", err), 500)
	}

	if updated {
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return handleHTTPError(h, fmt.Errorf("failed to encode value"), 500)
		}
		if err = db.Put(key, payloadJSON); err != nil {
			return handleHTTPError(h, fmt.Errorf("failed to update value"), 500)
		}
	}

	return sendJSONResponse(h, payload)
}

//export deleteValue
func deleteValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)
	if isPreflight(h) {
		return 0
	}

	peerID, err := getPeerIDFromRequest(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	key, _, err := findValueByPeerID(db, peerID)
	if err != nil {
		if errors.Is(err, errValueNotFound) {
			return handleHTTPError(h, err, 404)
		}
		return handleHTTPError(h, fmt.Errorf("failed to read value"), 500)
	}

	if err = db.Delete(key); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to delete value"), 500)
	}

	return sendJSONResponse(h, map[string]string{
		"peerId": peerID,
		"status": "deleted",
	})
}
