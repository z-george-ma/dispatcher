// config.go
package main

import (
	"gopkg.in/yaml.v2"
	"log"
	"io/ioutil"
	"os"
	"io"
)

type Config struct {
	Log string "log"
	Listen string
	Worker int
}

func readConfig() Config {
	config := Config{}
	f, err := os.OpenFile("config.yml", os.O_RDONLY, 400)
	if os.IsNotExist(err) {
		log.Fatal("config.yml not found")
		return config
	} else if err != nil {
		log.Fatal(err)
		return config
	}

	defer f.Close()
		
	return readConfigFromReader(f)
}

func readConfigFromReader(f io.Reader) Config {
	config := Config{}
	data, err := ioutil.ReadAll(f)
	
	if err != nil {
		log.Fatal(err)
		return config
	}
	
	
	if err = yaml.Unmarshal(data, &config); err != nil {
		log.Fatal(err)
	}
	
	return config
}
