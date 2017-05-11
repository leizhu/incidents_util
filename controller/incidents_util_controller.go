package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/bitly/go-simplejson"
	"github.com/robfig/cron"
	elastic "gopkg.in/olivere/elastic.v5"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

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
	log.Info("Reserved indices: " + strings.Join(reservedIndices, ", "))
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
func (ic IncidentsUtilController) Snapshot() {
	bytes, err := ic.loadConfig()
	if err != nil {
		return
	}
	js, _ := simplejson.NewJson(bytes)
	c := cron.New()
	spec, _ := js.Get("cron").String()
	//spec := "0 */1 * * * *"
	c.AddFunc(spec, func() {
		log.Info("========================")
		client, ctx, err := ic.es_client()
		if err != nil {
			log.Error("Can not connect to ES" + err.Error())
			return
		}
		arr, _ := js.Get("snapshot_indices").Array()
		for _, v := range arr {
			m := v.(map[string]interface{})
			index := m["index"].(string)
			repository := m["repository"].(string)
			snap_name := m["snap_name"].(string)
			log.Infof("---begin to snapshot index[%s] in repository[%s]", index, repository)
			ic.snapshot_index(repository, snap_name, index, client, ctx)
			log.Infof("---End to snapshot index[%s] in repository[%s]", index, repository)
		}
		client.Stop()
	})
	c.Start()
	select {}
}

func (ic IncidentsUtilController) snapshot_index(repository string, snap_name string, index string, client *elastic.Client, ctx context.Context) {
	body := fmt.Sprintf("{\"type\":\"fs\",\"settings\":{\"location\":\"%s\",\"compress\":true}}", repository)
	resp, err := client.SnapshotCreateRepository(repository).Repository(repository).BodyString(body).Pretty(true).Do(ctx)
	if err != nil || !resp.Acknowledged {
		log.Error("Create snapshot repository  error: " + err.Error())
		return
	}
	log.Infof("Create snapshot repository [%s] successfully!", repository)

	indices := ic.get_all_indices(index, client, ctx)
	ic.make_snap(repository, snap_name, strings.Join(indices, ","))
}

type SnapshotRequest struct {
	Indices            string `json:"indices"`
	IgnoreUnavailable  bool   `json:"ignore_unavailable"`
	IncludeGlobalState bool   `json:"include_global_state"`
}

type SnapshotResponse struct {
	Accepted bool `json:"accepted"`
}

func (ic IncidentsUtilController) make_snap(repository string, snap_name string, indices string) {
	snap_name = snap_name + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	body, _ := json.Marshal(SnapshotRequest{Indices: indices, IgnoreUnavailable: true, IncludeGlobalState: false})
	client := &http.Client{}
	url := fmt.Sprintf("%s/_snapshot/%s/%s", ic.ElasticsearchURL, repository, snap_name)
	log.Debug(url)
	log.Debug(string(body))
	req, err := http.NewRequest("PUT", url, strings.NewReader(string(body)))
	if err != nil {
		log.Errorf("Encouter error when sending request %s, error is %s", url, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Encouter error when sending request %s, error is %s", url, err.Error())
		return
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("Can not parse response body: " + err.Error())
		return
	}
	log.Debug(string(respBody))
	var snapResp SnapshotResponse
	err = json.Unmarshal(respBody, &snapResp)
	if err != nil || !snapResp.Accepted {
		log.Errorf("Snapshot indices[%s] failed!", indices)
	} else {
		log.Infof("Snapshot indices[%s] success, snapshot name is %s", indices, snap_name)
	}
}

func (ic IncidentsUtilController) get_all_indices(index string, client *elastic.Client, ctx context.Context) []string {
	resp, err := client.IndexGet().Index(index).Pretty(true).Do(ctx)
	ret := make([]string, 0)
	if err != nil {
		log.Errorf("Get all indices of [%s] error: %s", index, err.Error())
	} else {
		for i, _ := range resp {
			ret = append(ret, i)
		}
	}
	return ret
}
