/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package vector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type VectorUtil interface {
	CheckCollectionExists(collectionName string) (bool, error)
	CreateCollectionExists(collectionName, distance string, size int) error
}

type QdrantUtil struct {
	BaseURL string
}

func (qd *QdrantUtil) CheckCollectionExists(collectionName string) (bool, error) {
	qdrantURL := fmt.Sprintf("%scollections/%s", qd.BaseURL, collectionName)

	resp, err := http.Get(qdrantURL)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	body, _ := ioutil.ReadAll(resp.Body)
	return false, fmt.Errorf("Received error response: %s\n", string(body))
}

func (qd *QdrantUtil) CreateCollectionExists(collectionName, distance string, size int) error {
	qdrantURL := fmt.Sprintf("%scollections/%s", qd.BaseURL, collectionName)
	requestBody := map[string]interface{}{
		"vectors": map[string]interface{}{
			"size":     size,
			"distance": distance,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, qdrantURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("Received error response when create Qdrant collection: %s\n", resp.Status)
}
