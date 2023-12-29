package role

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func probe(parentCtx context.Context, timeout time.Duration, method string, url string, params map[string]any) (map[string]any, error) {
	timeoutCtx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	var reader io.Reader
	if params != nil {
		body, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("error marshalling params: %w", err)
		}
		reader = bytes.NewReader(body)
	}

	httpRequest, err := http.NewRequestWithContext(timeoutCtx, method, url, reader)
	if err != nil {
		return nil, fmt.Errorf("error creating request for url %v: %w", url, err)
	}

	resp, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusUnavailableForLegalReasons:
		return parseBody(resp.Body)
	case http.StatusNoContent:
		return nil, fmt.Errorf("status no content")
	default:
		msg, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %w", err)
		}
		return nil, fmt.Errorf("server responded with status code %d: %s", resp.StatusCode, msg)
	}
}

func parseBody(body io.Reader) (map[string]any, error) {
	result := map[string]any{}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed: %w", err)
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, fmt.Errorf("decode body failed: %w", err)
	}
	return result, nil
}
