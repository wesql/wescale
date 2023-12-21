package role

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func probe(parentCtx context.Context, method string, url string, params map[string]any) (map[string]any, error) {
	timeoutCtx, cancel := context.WithTimeout(parentCtx, mysqlRoleProbeTimeout)
	defer cancel()

	// prepare request
	var reader io.Reader = nil
	if params != nil {
		body, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(body)
	}
	httpRequest, err := http.NewRequestWithContext(timeoutCtx, method, url, reader)
	if err != nil {
		return nil, fmt.Errorf("Error creating request for url:%v, err: %v\n", url, err)
	}

	// send HTTP request
	resp, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("Error sending request: %v\n", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusUnavailableForLegalReasons:
		return parseBody(resp.Body)
	case http.StatusNoContent:
		return nil, fmt.Errorf("status no content")
	case http.StatusNotImplemented, http.StatusInternalServerError:
		fallthrough
	default:
		msg, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(string(msg))
	}
}

func parseBody(body io.Reader) (map[string]any, error) {
	result := map[string]any{}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed")
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, fmt.Errorf("decode body failed")
	}
	return result, nil
}
