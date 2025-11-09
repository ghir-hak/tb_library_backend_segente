package lib

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/taubyte/go-sdk/database"
	baseEvent "github.com/taubyte/go-sdk/event"
	httpEvent "github.com/taubyte/go-sdk/http/event"
)

const valueStorePrefix = "/values/"

type addressPayload struct {
	IP       string  `json:"ip"`
	Port     *string `json:"port,omitempty"`
	Protocol *string `json:"protocol,omitempty"`
}

type limitsPayload struct {
	Soft int `json:"soft"`
	Hard int `json:"hard"`
}

type serverDescriptor struct {
	PeerID  string         `json:"peerId"`
	Address addressPayload `json:"address"`
	Limits  limitsPayload  `json:"limits"`
	Raw     string         `json:"raw"`
}

type storedServer struct {
	serverDescriptor
	UpdatedAt string `json:"updatedAt"`
}

func setCORSHeaders(h httpEvent.Event) {
	h.Headers().Set("Access-Control-Allow-Origin", "*")
	h.Headers().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	h.Headers().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func handleHTTPError(h httpEvent.Event, err error, code int) uint32 {
	h.Headers().Set("Content-Type", "application/json")
	response := map[string]string{"error": err.Error()}
	jsonData, _ := json.Marshal(response)
	h.Write(jsonData)
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

func handlePreflight(h httpEvent.Event, expectedMethod string) (proceed bool, code uint32) {
	method, err := h.Method()
	if err != nil {
		return false, handleHTTPError(h, err, 400)
	}

	if method == "OPTIONS" {
		h.Return(204)
		return false, 0
	}

	if method != expectedMethod {
		h.Write([]byte("Method not allowed"))
		h.Return(405)
		return false, 1
	}

	return true, 0
}

//export listValues
func listValues(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)

	if proceed, code := handlePreflight(h, "GET"); !proceed {
		return code
	}

	db, err := database.New("seguente")
	if err != nil {
		return handleHTTPError(h, err, 500)
	}
	defer db.Close()

	keys, err := db.List(valueStorePrefix)
	if err != nil {
		return handleHTTPError(h, err, 500)
	}

	records := make([]storedServer, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, valueStorePrefix) {
			key = valueStorePrefix + key
		}

		valueBytes, err := db.Get(key)
		if err != nil {
			continue
		}

		var record storedServer
		if err = json.Unmarshal(valueBytes, &record); err != nil {
			continue
		}

		records = append(records, record)
	}

	return sendJSONResponse(h, records)
}

//export registerValue
func registerValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)

	if proceed, code := handlePreflight(h, "POST"); !proceed {
		return code
	}

	body, err := io.ReadAll(h.Body())
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	var payload serverDescriptor
	if err = json.Unmarshal(body, &payload); err != nil {
		return handleHTTPError(h, err, 400)
	}

	if validationErr := validateServerDescriptor(payload); validationErr != nil {
		return handleHTTPError(h, validationErr, 400)
	}

	db, err := database.New("seguente")
	if err != nil {
		return handleHTTPError(h, err, 500)
	}
	defer db.Close()

	record := storedServer{
		serverDescriptor: payload,
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return handleHTTPError(h, err, 500)
	}

	if err = db.Put(valueStorePrefix+payload.PeerID, recordBytes); err != nil {
		return handleHTTPError(h, err, 500)
	}

	return sendJSONResponse(h, record)
}

//export getValue
func getValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)

	if proceed, code := handlePreflight(h, "GET"); !proceed {
		return code
	}

	key, err := getPeerIDFromPath(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := database.New("seguente")
	if err != nil {
		return handleHTTPError(h, err, 500)
	}
	defer db.Close()

	valueBytes, err := db.Get(valueStorePrefix + key)
	if err != nil {
		return handleHTTPError(h, errors.New("Server descriptor not found"), 404)
	}

	var record storedServer
	if err = json.Unmarshal(valueBytes, &record); err != nil {
		return handleHTTPError(h, err, 500)
	}

	return sendJSONResponse(h, record)
}

//export deleteValue
func deleteValue(e baseEvent.Event) uint32 {
	h, err := e.HTTP()
	if err != nil {
		return 1
	}
	setCORSHeaders(h)

	if proceed, code := handlePreflight(h, "DELETE"); !proceed {
		return code
	}

	key, err := getPeerIDFromPath(h)
	if err != nil {
		return handleHTTPError(h, err, 400)
	}

	db, err := database.New("seguente")
	if err != nil {
		return handleHTTPError(h, err, 500)
	}
	defer db.Close()

	valueKey := valueStorePrefix + key
	if _, err = db.Get(valueKey); err != nil {
		return handleHTTPError(h, errors.New("Server descriptor not found"), 404)
	}

	if err = db.Delete(valueKey); err != nil {
		return handleHTTPError(h, err, 500)
	}

	return sendJSONResponse(h, map[string]string{"message": "Server descriptor deleted"})
}

func validateServerDescriptor(payload serverDescriptor) error {
	if payload.PeerID == "" {
		return errors.New("Missing peerId field")
	}
	if payload.Address.IP == "" {
		return errors.New("Missing address.ip field")
	}
	if payload.Limits.Soft == 0 && payload.Limits.Hard == 0 {
		return errors.New("Missing limits field")
	}
	return nil
}

func getPeerIDFromPath(h httpEvent.Event) (string, error) {
	path, err := h.Path()
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", errors.New("Missing peerId in path")
	}
	return parts[len(parts)-1], nil
}
