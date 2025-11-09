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

func openDB() (*database.Database, error) {
	return database.New(dbName)
}

func isPreflight(h httpEvent.Event) bool {
	if method, err := h.Method(); err == nil && method == "OPTIONS" {
		h.Return(204)
		return true
	}
	return false
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

	for i := range keys {
		keys[i] = strings.TrimPrefix(keys[i], valueStorePrefix)
	}

	return sendJSONResponse(h, map[string]interface{}{
		"count": len(keys),
		"keys":  keys,
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

	db, err := openDB()
	if err != nil {
		return handleHTTPError(h, fmt.Errorf("failed to open database"), 500)
	}
	defer db.Close()

	id := fmt.Sprintf("%d", time.Now().UnixNano())
	if err = db.Put(valueStorePrefix+id, body); err != nil {
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

	id, err := h.Path().Get("id")
	if err != nil || id == "" {
		return handleHTTPError(h, fmt.Errorf("missing value id"), 400)
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

	id, err := h.Path().Get("id")
	if err != nil || id == "" {
		return handleHTTPError(h, fmt.Errorf("missing value id"), 400)
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
