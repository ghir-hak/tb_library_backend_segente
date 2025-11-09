package lib

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/taubyte/go-sdk/database"
	baseEvent "github.com/taubyte/go-sdk/event"
	httpEvent "github.com/taubyte/go-sdk/http/event"
)

const (
	dbName           = "seguente"
	valueStorePrefix = "/values/"
)

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

type valueRecord struct {
	ID string `json:"id"`
	valuePayload
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

func getIDFromPath(h httpEvent.Event) (string, error) {
	path, err := h.Path()
	if err != nil {
		return "", fmt.Errorf("failed to read path")
	}

	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "", fmt.Errorf("missing value id")
	}

	segments := strings.Split(trimmed, "/")
	id := segments[len(segments)-1]
	if id == "" {
		return "", fmt.Errorf("missing value id")
	}

	return id, nil
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

	values := make([]valueRecord, 0, len(keys))
	for _, key := range keys {
		id := strings.TrimPrefix(key, valueStorePrefix)
		data, err := db.Get(key)
		if err != nil {
			return handleHTTPError(h, fmt.Errorf("failed to read value with id %s", id), 500)
		}

		var payload valuePayload
		if err = json.Unmarshal(data, &payload); err != nil {
			return handleHTTPError(h, fmt.Errorf("stored value for id %s is invalid", id), 500)
		}

		values = append(values, valueRecord{
			ID:           id,
			valuePayload: payload,
		})
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

	id := fmt.Sprintf("%d", time.Now().UnixNano())

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to encode payload"), 500)
	}

	if err = db.Put(valueStorePrefix+id, payloadJSON); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to store value"), 500)
	}

	return sendJSONResponse(h, map[string]string{
		"id":     id,
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

	id, err := getIDFromPath(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	data, err := db.Get(valueStorePrefix + id)
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("value not found"), 404)
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

	id, err := getIDFromPath(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	key := valueStorePrefix + id
	if _, err = db.Get(key); err != nil {
		return handleHTTPError(h, fmt.Errorf("value not found"), 404)
	}

	if err = db.Delete(key); err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to delete value"), 500)
	}

	return sendJSONResponse(h, map[string]string{
		"id":     id,
		"status": "deleted",
	})
}
