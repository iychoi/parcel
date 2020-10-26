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

package cli

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/tkanos/gonfig"
)

const (
	// ParcelConfigPath is a default path for config file
	ParcelConfigPath = "/etc/parcel.config"
)

// Config object contains configuration
type Config struct {
	CatalogServiceURL    string `json:"catalogServiceURL"`
	Namespace            string `json:"namespace"`
	KubernetesConfigPath string `json:"kubernetesConfigPath"`
}

// GetConfig returns Config object
func GetConfig() (*Config, error) {
	config := Config{}
	err := gonfig.GetConf(ParcelConfigPath, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// CreateConfig saves config
func CreateConfig(config *Config) error {
	jsonBytes, _ := json.Marshal(config)
	err := ioutil.WriteFile(ParcelConfigPath, jsonBytes, 0644)
	if err != nil {
		return err
	}

	return nil
}

// CheckConfig checks existance of config
func CheckConfig() bool {
	_, err := os.Stat(ParcelConfigPath)
	return !os.IsNotExist(err)
}
