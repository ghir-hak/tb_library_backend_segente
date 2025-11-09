package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/taubyte/go-sdk/database"
	baseEvent "github.com/taubyte/go-sdk/event"
	httpEvent "github.com/taubyte/go-sdk/http/event"
)

const (
	dbName           = "seguentedb"
	valueStorePrefix = ""
)

var errValueNotFound = errors.New("value not found")

type valueAddress struct {
	IP       string  `json:"ip"`
	Port     *string `json:"port"`
	Protocol *string `json:"protocol"`
}

type valueLimits struct {
	Soft float64 `json:"soft"`
	Hard float64 `json:"hard"`
}

type valuePayload struct {
	PeerID  string       `json:"peerId"`
	Address valueAddress `json:"address"`
	Limits  valueLimits  `json:"limits"`
	Raw     string       `json:"raw"`
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
	return nil
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
	key := valueStorePrefix + peerID
	if data, err := db.Get(key); err == nil {
		return key, data, nil
	}

	keys, err := db.List(valueStorePrefix)
	if err != nil {
		return "", nil, err
	}

	for _, k := range keys {
		data, err := db.Get(k)
		if err != nil {
			continue
		}

		var payload valuePayload
		if err = json.Unmarshal(data, &payload); err != nil {
			continue
		}

		if strings.TrimSpace(payload.PeerID) == strings.TrimSpace(peerID) && payload.PeerID != "" {
			return k, data, nil
		}
	}

	return "", nil, errValueNotFound
}

func cleanupLegacyEntries(db database.Database, peerID, keepKey string) error {
	keys, err := db.List(valueStorePrefix)
	if err != nil {
		return err
	}

	for _, k := range keys {
		if k == keepKey {
			continue
		}

		data, err := db.Get(k)
		if err != nil {
			continue
		}

		var payload valuePayload
		if err = json.Unmarshal(data, &payload); err != nil {
			continue
		}

		if strings.TrimSpace(payload.PeerID) == strings.TrimSpace(peerID) && payload.PeerID != "" {
			if err = db.Delete(k); err != nil {
				return err
			}
		}
	}

	return nil
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

	keys, err := db.List(valueStorePrefix)
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to list values"), 500)
	}

	values := make([]valuePayload, 0, len(keys))
	for _, key := range keys {
		id := strings.TrimPrefix(key, valueStorePrefix)
		data, err := db.Get(key)
		if err != nil {
			return handleHTTPError(h, fmt.Errorf("failed to read value for key %s", id), 500)
		}

		var payload valuePayload
		if err = json.Unmarshal(data, &payload); err != nil {
			return handleHTTPError(h, fmt.Errorf("stored value for key %s is invalid", id), 500)
		}

		if strings.TrimSpace(payload.PeerID) == "" {
			payload.PeerID = id
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

	if err = validateValuePayload(payload); err != nil {
		return handleHTTPError(h, err, 400)
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

	key := valueStorePrefix + peerID
	if err = db.Put(key, payloadJSON); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to store value"), 500)
	}

	if err = cleanupLegacyEntries(db, peerID, key); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to cleanup legacy entries"), 500)
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

	_, data, err := findValueByPeerID(db, peerID)
	if err != nil {
		if errors.Is(err, errValueNotFound) {
			return handleHTTPError(h, err, 404)
		}
		return handleHTTPError(h, fmt.Errorf("failed to read value"), 500)
	}

	h.Headers().Set("Content-Type", "application/json")
	h.Write(data)
	h.Return(200)
	return 0
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
