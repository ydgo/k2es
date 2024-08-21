package data

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"net/http"
)

const (
	TestIndex    = "k2es"
	IndexMapping = `"mappings" : {
      "properties" : {
        "_app" : {
          "type" : "keyword"
        },
        "_datamodel" : {
          "type" : "keyword"
        },
        "_host" : {
          "type" : "keyword"
        },
        "_indextime" : {
          "type" : "date",
          "format" : "epoch_millis"
        },
        "_raw" : {
          "type" : "text"
        },
        "_sourceid" : {
          "type" : "keyword"
        },
        "_sourcename" : {
          "type" : "keyword"
        },
        "_time" : {
          "type" : "date",
          "format" : "epoch_millis"
        },
        "_uuid" : {
          "type" : "keyword"
        }
      }
    }`
	TestMessage = `{
  "_host": "10.212.134.2",
  "_sourcename": "k2es数据源",
  "_sourceid": "ae62e244-7dc8-4ae4-b135-ce1166d761fe",
  "_time": 1724224571316,
  "_raw": "uuid1716102090303|1716102090|2961712fad047809|01|02|01|-|OPPO PFFM10|-|-|2.7.2|-|306|{\"code\":200,\"baseUrl\":\"https://partition55.wiodo.tech:443/\",\"path\":\"api/v2/devices?shared\u003d1\",\"time\":\"1037ms\"}|21096b3f0f474920a2faf8c52312a43b|-|-|-|0|0|0|0|default|0|-|播放信息 id: 103H9WP00L0JGH10V9P4392201f6d1b4436da455ca335e697bd5 uri: mpp://191779 arg: mpp://191779 config: {mqtt_host: partition1.wiodo.tech, mqtt_port: 31883, mqtt_user: emqx, mqtt_pass: Admin@123456,./, mqtt_topic: p2p, stun_host: turn01.wiodo.tech, stun_port: 33468, turn_user: , turn_pass: , turn_host: turn01.wiodo.tech, turn_port: 33468, key_url: https://partition1.wiodo.tech:443/api/v2/devices/103H9WP00L0JGH10V9P4392201f6d1b4436da455ca335e697bd5/keys?token\u003d79c8b4e8553bdc37c27441c0da614ed3, headers: {x-user-id: 392201f6d1b4436da455ca335e697bd5, x-user-name: 15805124069, x-app-version: 2.5.2}, denoise: false} 起播时间ms: 1759|BLQ_PDD",
  "_indextime": 1724224571316,
  "_appname": "",
  "_datamodel": "默认日志",
  "_uuid": "673eccfd-8617-4af4-8d9b-ebaccf3eb9ab"
}`
)

func CreateIndex(es *elasticsearch.Client) {
	res, err := es.Indices.Exists([]string{TestIndex})
	if err != nil {
		log.Fatalln("Error checking if index exists1: ", err)
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return
	} else if res.StatusCode != http.StatusNotFound {
		log.Fatalln("Error checking if index exists: ", res.Status())
	}
	res, err = es.Indices.Create(TestIndex, func(request *esapi.IndicesCreateRequest) {
		request.Body = bytes.NewReader([]byte(IndexMapping))
	})
	//req := esapi.IndicesCreateRequest{
	//	Index: TestIndex,
	//	Body:  bytes.NewReader([]byte(IndexMapping)),
	//}
	//res, err = req.Do(context.Background(), es)
	//if err != nil {
	//	log.Fatalln("Error creating index: ", err)
	//}
	defer res.Body.Close()
	if res.IsError() {
		log.Fatalln("Error creating index: ", res.Status())
	}
	req1 := esapi.CreateRequest{
		Index: TestIndex,
		Body:  bytes.NewReader([]byte(TestMessage)),
	}
	res, err = req1.Do(context.Background(), es)
	if err != nil {
		log.Fatalln("Error creating document: ", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Fatalln("Error creating document: ", res.Status())
	}
}
