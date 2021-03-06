package main

import (
	"flag"
	"github.com/leizhu/incidents_util/controller"
	"github.com/leizhu/incidents_util/logutil"
	"log"
)

var (
	operation_type = flag.String(
		"operation.type", "cleanup",
		"operation to ElasticSearch, please set cleanup or snapshot",
	)
	cleanup_config_file = flag.String(
		"cleanup.config", "/opt/skyguard/cleanup-es.json",
		"config file",
	)
	es_url = flag.String(
		"elasticsearch.url", "http://elasticsearch:9200",
		"URL of elasticsearch",
	)
	log_level = flag.String(
		"loglevel", "INFO",
		"log level",
	)
)

func main() {
	flag.Parse()
	logutil.Init(*log_level)

	log.Println("elasticsearch.url: ", *es_url)
	log.Println("operation.type: ", *operation_type)
	log.Println("config.file: ", *cleanup_config_file)
	log.Println("log level: ", *log_level)
	if *operation_type == "" || (*operation_type != "cleanup" && *operation_type != "snapshot") {
		log.Println("Please specify operation type, cleanup or snapshot")
		return
	}
	ic := controller.NewIncidentsUtilController(*es_url, *cleanup_config_file)
	if *operation_type == "cleanup" {
		ic.Cleanup()
	}
	if *operation_type == "snapshot" {
		ic.Snapshot()
	}
}
