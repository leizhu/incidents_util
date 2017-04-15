package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/bitly/go-simplejson"
	"github.com/leizhu/incidents_util/logutil"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

func InitLog(loglevel string) {
	log.SetFormatter(&log.JSONFormatter{})
	//log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	switch loglevel {
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
	log.AddHook(logutil.ContextHook{})
}

type (
	IncidentsUtilController struct {
		ElasticsearchURL string
		ConfigFile       string
	}
)

func NewIncidentsUtilController(es_url string, configFile string) *IncidentsUtilController {
	return &IncidentsUtilController{ElasticsearchURL: es_url, ConfigFile: configFile}
}

func (ic *IncidentsUtilController) loadConfig() ([]byte, error) {
	bytes, err := ioutil.ReadFile(ic.ConfigFile)
	if err != nil {
		log.Error("Read config error: ", err.Error())
		return nil, err
	}
	log.Info("Config is: " + string(bytes))
	return bytes, nil
}

func (ic IncidentsUtilController) es_client() (*elastic.Client, context.Context, error) {
	ctx := context.Background()
	client, err := elastic.NewClient(elastic.SetURL(ic.ElasticsearchURL), elastic.SetSniff(true))
	if err != nil {
		log.Error("Can not create es client: " + err.Error())
		return nil, nil, errors.New(fmt.Sprintln("Can not create es client: ", err))
	}
	info, code, err := client.Ping(ic.ElasticsearchURL).Do(ctx)
	if err != nil {
		log.Error("Elasticsearch returned with code %d and version %s", code, info.Version.Number)
		return nil, nil, errors.New(fmt.Sprintln("Elasticsearch returned with code %d and version %s", code, info.Version.Number))
	}
	return client, ctx, nil
}

func (ic IncidentsUtilController) Cleanup() {
	bytes, err := ic.loadConfig()
	if err != nil {
		return
	}
	js, _ := simplejson.NewJson(bytes)
	interval, _ := js.Get("check_interval").Int64()
	duration, _ := time.ParseDuration(fmt.Sprintf("%ds", interval))
	for {
		log.Info("========================")
		log.Info("---begin---")
		client, ctx, err := ic.es_client()
		if err != nil {
			log.Error("Can not connect to ES" + err.Error())
			return
		}
		arr, _ := js.Get("clean_indices").Array()
		for _, v := range arr {
			m := v.(map[string]interface{})
			index_prefix := m["index"].(string)
			time_series, _ := strconv.Atoi(string(m["time_series"].(json.Number)))
			ic.clean_index(index_prefix, time_series, client, ctx)
		}
		log.Info("---end---")
		time.Sleep(duration)
	}
}

func (ic IncidentsUtilController) clean_index(indexPrefix string, lastDays int, client *elastic.Client, ctx context.Context) {
	reservedIndices := getReservedIndices(indexPrefix, lastDays)
	resp, err := client.IndexGet().Index(indexPrefix + "*").Human(true).Pretty(true).Feature("_aliases").Do(ctx)
	if err != nil {
		log.Error("Can not get existing indices in ES" + err.Error())
		return
	}
	deletedIndices := []string{}
	for esIndex, _ := range resp {
		if !stringInSlice(esIndex, reservedIndices) {
			deletedIndices = append(deletedIndices, esIndex)
		}
	}
	if len(deletedIndices) > 0 {
		log.Info("Delete below indices: " + strings.Join(deletedIndices, ", "))
		resp, err := client.DeleteIndex().Index(deletedIndices).Pretty(true).Do(ctx)
		if err != nil {
			log.Error("Delete index error: " + err.Error())
			return
		}
		if !resp.Acknowledged {
			log.Error("Delete index error: " + err.Error())
			return
		}
		log.Info("Delete indices successful!")
	}
}

func getReservedIndices(indexPrefix string, lastDays int) []string {
	reservedIndices := []string{}
	preDay, _ := time.ParseDuration("-24h")
	t := time.Now()
	reservedIndices = append(reservedIndices, indexPrefix+"-"+t.Format("2006.01.02"))
	for i := 0; i < lastDays-1; i++ {
		t = t.Add(preDay)
		reservedIndices = append(reservedIndices, indexPrefix+"-"+t.Format("2006.01.02"))
	}
	log.Debug(reservedIndices)
	return reservedIndices
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
func (ic IncidentsUtilController) Snapshot() {}
