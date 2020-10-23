/*
Copyright 2020 CyVerse
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package catalog

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/iychoi/parcel-catalog-service/pkg/dataset"
)

const (
	// CatalogServiceURL is a default catalog service URL
	CatalogServiceURL = "http://localhost:8080"

	// ShortDescriptionLen is the short description max length
	ShortDescriptionLen = 200
)

// ParcelCatalogServiceClient is a client for catalog service
type ParcelCatalogServiceClient struct {
	catalogServiceURL string
	trace             bool
	restClient        *resty.Request
}

// NewCatalogServiceClient creates a new ParcelCatalogServiceClient
func NewCatalogServiceClient(CatalogServiceURL string, trace bool) (*ParcelCatalogServiceClient, error) {
	serviceURL := CatalogServiceURL
	if len(CatalogServiceURL) > 0 {
		serviceURL = CatalogServiceURL
	}

	return &ParcelCatalogServiceClient{
		catalogServiceURL: serviceURL,
		restClient:        getRestClient(trace),
		trace:             trace,
	}, nil
}

func getRestClient(trace bool) *resty.Request {
	restClient := resty.New()
	req := restClient.R()
	if trace {
		req = req.EnableTrace()
	}
	return req
}

func traceResponse(trace bool, resp *resty.Response, err error) {
	if trace {
		// Explore response object
		log.Println("Response Info:")
		log.Println("  Error      :", err)
		log.Println("  Status Code:", resp.StatusCode())
		log.Println("  Status     :", resp.Status())
		log.Println("  Proto      :", resp.Proto())
		log.Println("  Time       :", resp.Time())
		log.Println("  Received At:", resp.ReceivedAt())
		log.Println("  Body       :\n", resp)
		log.Println()

		// Explore trace info
		log.Println("Request Trace Info:")
		ti := resp.Request.TraceInfo()
		log.Println("  DNSLookup     :", ti.DNSLookup)
		log.Println("  ConnTime      :", ti.ConnTime)
		log.Println("  TCPConnTime   :", ti.TCPConnTime)
		log.Println("  TLSHandshake  :", ti.TLSHandshake)
		log.Println("  ServerTime    :", ti.ServerTime)
		log.Println("  ResponseTime  :", ti.ResponseTime)
		log.Println("  TotalTime     :", ti.TotalTime)
		log.Println("  IsConnReused  :", ti.IsConnReused)
		log.Println("  IsConnWasIdle :", ti.IsConnWasIdle)
		log.Println("  ConnIdleTime  :", ti.ConnIdleTime)
	}
}

func (client *ParcelCatalogServiceClient) get(url string) (*resty.Response, error) {
	resp, err := client.restClient.Get(url)
	traceResponse(client.trace, resp, err)
	return resp, err
}

// GetAllDatasets returns all datasets
func (client *ParcelCatalogServiceClient) GetAllDatasets() ([]*dataset.Dataset, error) {
	requestURL := makeRequestPath(CatalogServiceURL, "/datasets")

	resp, err := client.get(requestURL)
	if err != nil {
		return nil, err
	}

	body := resp.Body()
	datasets := dataset.Listify(body)

	return datasets, nil
}

// SearchDatasets returns search result
func (client *ParcelCatalogServiceClient) SearchDatasets(keywords []string) ([]*dataset.Dataset, error) {
	// TODO: add search API to catalog service
	// Now just do it from local

	datasets, err := client.GetAllDatasets()
	if err != nil {
		log.Fatal(err)
	}

	foundDatasets := []*dataset.Dataset{}
	for _, ds := range datasets {
		if ds.ContainsKeywords(keywords) {
			foundDatasets = append(foundDatasets, ds)
		}
	}
	return foundDatasets, nil
}

// SelectDatasets returns datasets with specific IDs
func (client *ParcelCatalogServiceClient) SelectDatasets(ids []string) ([]*dataset.Dataset, error) {
	// TODO: add search API to catalog service
	// Now just do it from local

	datasets, err := client.GetAllDatasets()
	if err != nil {
		log.Fatal(err)
	}

	foundDatasets := []*dataset.Dataset{}
	for _, ds := range datasets {
		for _, id := range ids {
			if strconv.FormatInt(ds.ID, 10) == id {
				// found
				foundDatasets = append(foundDatasets, ds)
				break
			}
		}
	}
	return foundDatasets, nil
}

func makeRequestPath(requestRoot string, path string) string {
	if strings.HasSuffix(requestRoot, "/") && strings.HasPrefix(path, "/") {
		return fmt.Sprintf("%s%s", strings.TrimRight(requestRoot, "/"), path)
	} else if !strings.HasSuffix(requestRoot, "/") && !strings.HasPrefix(path, "/") {
		return fmt.Sprintf("%s/%s", requestRoot, path)
	} else {
		return fmt.Sprintf("%s%s", requestRoot, path)
	}
}
