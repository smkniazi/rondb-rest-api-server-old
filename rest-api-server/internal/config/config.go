/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/version"
)

const CONFIG_FILE_NAME = "config.json"

var _config RSConfiguration

type RSConfiguration struct {
	RestServer  RestServer
	RonDBConfig RonDB
	MySQLServer MySQLServer
	Log         log.LogConfig
}

func (config RSConfiguration) String() string {
	b, _ := json.MarshalIndent(_config, "", "\t")
	return fmt.Sprintf("%s", string(b))
}

type RestServer struct {
	IP              string
	Port            uint16
	APIVersion      string
	BufferSize      int
	PreAllocBuffers uint32
	GOMAXPROCS      int
}

type MySQLServer struct {
	IP       string
	Port     uint16
	User     string
	Password string
}

type RonDB struct {
	IP   string
	Port uint16
}

func init() {
	restServer := RestServer{
		IP:              "localhost",
		Port:            8080,
		APIVersion:      version.VERSION,
		BufferSize:      320 * 1024,
		GOMAXPROCS:      -1,
		PreAllocBuffers: 1024,
	}

	ronDBConfig := RonDB{
		IP:   "localhost",
		Port: 1186,
	}

	mySQLServer := MySQLServer{
		IP:       "localhost",
		Port:     3306,
		User:     "rondb",
		Password: "rondb",
	}

	log := log.LogConfig{
		Level:      "info",
		Filename:   "",
		MaxSizeMB:  100,
		MaxBackups: 10,
		MaxAge:     30,
	}

	_config = RSConfiguration{
		RestServer:  restServer,
		MySQLServer: mySQLServer,
		RonDBConfig: ronDBConfig,
		Log:         log,
	}

	dir, err := os.Getwd()
	if err == nil {
		configFile := filepath.Join(dir, CONFIG_FILE_NAME)
		LoadConfig(configFile, false)
	}
}

func LoadConfig(path string, fail bool) {
	jsonFile, err := os.Open(path)
	if err != nil {
		if fail {
			panic(fmt.Sprintf("Unable to read configuration file. Error: %v", err))
		}
		return
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	err = json.Unmarshal([]byte(byteValue), &_config)
	if err != nil {
		panic(fmt.Sprintf("Unable to load configuration from file. Error: %v", err))
	}
}

func PrintConfig() {
	b, _ := json.MarshalIndent(_config, "", "\t")
	log.Infof("Configuration loaded from file: %s\n", string(b))
}

func Configuration() RSConfiguration {
	return _config
}
