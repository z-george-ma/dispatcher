// config.go
package main

import (
	"os"
	"strconv"
)

type Config struct {
	Log string
	DeadLetter string
	Listen string
	Worker int
	RetryLimit int
	DefaultTimeout int
}

func getEnvOrDefault(name string, defaultValue string) string {
	value := os.Getenv(name)
	
	if value == "" {
		value = defaultValue
	}
	
	return value
}


func getEnvOrDefaultInt(name string, defaultValue int) int {
	strValue := os.Getenv(name)
	
	if strValue == "" {
		return defaultValue
	}
	
	intV, err := strconv.Atoi(strValue)
	
	if err != nil {
		return defaultValue
	}
	
	return intV
}



func readConfig() Config {
	return Config{
		Log: getEnvOrDefault("TRANSACTION_LOG", "transaction.log"),
		DeadLetter: getEnvOrDefault("DEAD_LETTER", "deadletter.log"),
		Listen: getEnvOrDefault("LISTEN", ":80"),
		Worker: getEnvOrDefaultInt("WORKER", 10),
		RetryLimit: getEnvOrDefaultInt("RETRY_LIMIT", 10),
		DefaultTimeout: getEnvOrDefaultInt("DEFAULT_TIMEOUT", 10000),
	}
}