package main

import (
	"testing"
	"strings"
)

func TestReadConfig(t *testing.T) {
	str := `listen: ":80"
log: transaction.log
worker: 10`

	config := readConfigFromReader(strings.NewReader(str))
	
	if config.Listen != ":80" {
		t.Error("Read config error - unexpected listen value")
	}
		
	if config.Log != "transaction.log" {
		t.Error("Read config error - unexpected log value")
	}	
	
	if config.Worker != 10 {
		t.Error("Read config error - unexpected worker value")
	}
}
