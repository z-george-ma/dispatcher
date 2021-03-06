package main

import (
	"testing"
	"os"
)

func TestReadConfigFromEnv(t *testing.T) {
	os.Setenv("TRANSACTION_LOG", "abc")
	os.Setenv("LISTEN", "def")
	os.Setenv("WORKER", "111")
	os.Setenv("RETRY_LIMIT", "222")
	os.Setenv("DEFAULT_TIMEOUT", "333")
	os.Setenv("EVENT_SOURCING", "true")
	
	config := readConfig()
	
	if config.Log != "abc" {
		t.Error("Read config error - unexpected log value")
	}	
	
	if config.Listen != "def" {
		t.Error("Read config error - unexpected listen value")
	}
		
	if config.Worker != 111 {
		t.Error("Read config error - unexpected worker value")
	}

	if config.RetryLimit != 222 {
		t.Error("Read config error - unexpected worker value")
	}
	
	if config.DefaultTimeout != 333 {
		t.Error("Read config error - unexpected worker value")
	}
	
	if config.EventSourcing != true {
		t.Error("Read config error - unexpected worker value")
	}
	
	os.Unsetenv("TRANSACTION_LOG")
	os.Unsetenv("LISTEN")
	os.Unsetenv("WORKER")
	os.Unsetenv("RETRY_LIMIT")
	os.Unsetenv("DEFAULT_TIMEOUT")
}


func TestReadConfigDefaultValue(t *testing.T) {
	config := readConfig()
	
	if config.Log != "transaction.log" {
		t.Error("Read config error - unexpected log value")
	}	
	
	if config.Listen != ":80" {
		t.Error("Read config error - unexpected listen value")
	}
		
	if config.Worker != 10 {
		t.Error("Read config error - unexpected worker value")
	}

	if config.RetryLimit != 10 {
		t.Error("Read config error - unexpected worker value")
	}

	if config.DefaultTimeout != 10000 {
		t.Error("Read config error - unexpected worker value")
	}

	if !config.EventSourcing {
		t.Error("Read config error - unexpected worker value")
	}
}
