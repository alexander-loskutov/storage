package storage

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const CONFIG_KEY = "--config"
const DEFAULT_CONFIG_LOCATION = "cfg/config.yaml"

func parseArgs() map[string]string {
	result := make(map[string]string)
	args := os.Args[1:]
	for _, arg := range args {
		split := strings.Split(arg, "=")
		if len(split) == 2 {
			result[split[0]] = split[1]
		}
	}

	return result
}

func getArg(args map[string]string, key string, fallback string) string {
	value, ok := args[CONFIG_KEY]
	if ok {
		return value
	}

	return fallback
}

func getConfigLocation(args map[string]string) string {
	path := getArg(args, CONFIG_KEY, DEFAULT_CONFIG_LOCATION)
	return resolvePath(path)
}

func StartApp() {
	log.Println("Starting the application...")
	args := parseArgs()
	configLocation := getConfigLocation(args)
	config := ReadConfiguration(configLocation)

	dao := NewDao(config)
	svc := NewPromotionsService(dao.Promotions)
	api := NewApi(config, svc)
	go api.Listen()
	log.Println("API started")

	consumer := NewFileConsumer()
	updater := NewStorageUpdater(config, consumer, svc)
	go updater.start()
	log.Println("Storage updater started")

	onShutdown := func() {
		err := dao.Close()
		if err != nil {
			log.Printf("Failed to close database connection: %v\n", err)
		}
	}

	end := shutdown(onShutdown)
	<-end
}

func shutdown(callback func()) chan bool {
	end := make(chan bool)

	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-s
		log.Println("Shutting down the application...")
		callback()
		close(end)
	}()

	return end
}
