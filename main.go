package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"github.com/Shopify/sarama"
	"github.com/ctdk/goas/v2/logger"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type Kairosdb struct {
	client *http.Client
	host   string
}

func NewKairosdb(host string) (*Kairosdb, error) {
	log.Printf("initializing kairosdb client to %s", host)
	return &Kairosdb{
		client: &http.Client{Timeout: (10 * time.Second)},
		host:   host,
	}, nil
}

type Datapoint struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     float32           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

//[{"Timestamp": 1457574354027, "Host": "172.17.0.5", "Attrs": {"r_s": 0.0, "_util": 0.48, "avgqu_sz": 0.01, "rrqm_s": 0.0, "await": 0.5, "avgrq_sz": 19.82, "r_await": 0.0, "kB_wrtn": 1100.0, "wrqm_s": 6.4, "svctm": 0.43, "wkB_s": 110.0, "kB_wrtn_s": 110.0, "kB_read_s": 0.0, "rkB_s": 0.0, "kB_read": 0.0, "tps": 11.1, "w_s": 11.1, "w_await": 0.5}, "Name": "de1807c934b3-sdb", "ID": "sdb"}]

type KafkaMessage struct {
	Timestamp int64
	Host      string
	Attrs     map[string]float32
	Name      string
	ID        string
}

func KafkaMessageToDatapoints(m *sarama.ConsumerMessage) ([]Datapoint, error) {
	datapoints := make([]Datapoint, 0, 18)

	kms := make([]KafkaMessage, 0, 1)
	jsonBlob := m.Value
	err := json.Unmarshal(jsonBlob, &kms)
	if err != nil {
		log.Println("err:", err)
		log.Println(".. jsonBlob:", string(jsonBlob))
		return datapoints, err
	}
	if len(kms) > 1 {
		log.Println("..", len(kms), "kms:", kms)
	}
	km := kms[0]
	logger.Debugf("km: %+v", km)

	tags := make(map[string]string)
	tags["topic"] = m.Topic
	key := string(m.Key)
	if len(key) > 0 {
		tags["key"] = key
	}
	tags["host"] = km.Host
	tags["id"] = km.ID
	tags["name"] = km.Name

	for k, v := range km.Attrs {
		dp := Datapoint{
			Name:      m.Topic + "." + k,
			Timestamp: km.Timestamp,
			Value:     v,
			Tags:      tags,
		}
		logger.Debugf("dp: %+v", dp)
		datapoints = append(datapoints, dp)
	}

	return datapoints, nil
}

func (kdb *Kairosdb) SendKafkaMessages(ms []*sarama.ConsumerMessage) error {
	datapoints := make([]Datapoint, 0, len(ms))
	for _, m := range ms {
		dps, err := KafkaMessageToDatapoints(m)
		if err == nil {
			datapoints = append(datapoints, dps...)
		}
	}
	return kdb.AddDatapoints(datapoints)
}

func (kdb *Kairosdb) SendKafkaMessage(m *sarama.ConsumerMessage) error {
	ms := make([]*sarama.ConsumerMessage, 1)
	ms[0] = m
	return kdb.SendKafkaMessages(ms)
}

// AddDatapoints add datapoints to configured kairosdb instance
func (kdb *Kairosdb) AddDatapoints(datapoints []Datapoint) error {
	json, err := json.Marshal(datapoints)
	if err != nil {
		return err
	}
	logger.Debugf("%s", string(json))
	resp, err := kdb.client.Post(kdb.host+"/api/v1/datapoints", "application/json", bytes.NewBuffer(json))
	if err != nil {
		// error doing the request. retry later
		return err
	}
	if resp.StatusCode != 204 {
		return errors.New(resp.Status)
	}
	return nil
}

var (
	kfhost   *string = flag.String("kf", "localhost:9092", "Kafka Brokers")
	kftopics *string = flag.String("kt", "test", "Kafka Topics")
	kdbhost  *string = flag.String("kdb", "http://localhost:8080", "KairosDB (REST)")
	debug    *bool   = flag.Bool("debug", false, "Debug")
)

func StartWorker(topic string) {
	log.Printf("starting worker for topic: %s", topic)
	kdb, err := NewKairosdb(*kdbhost)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumer([]string{*kfhost}, nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
		logger.Debugf("consumer closed")
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
		logger.Debugf("partitionConsumer closed")
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed msg [top]%s [par]%d [off]%d [key]%s [val]%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			err = kdb.SendKafkaMessage(msg)
			if err != nil {
				log.Printf("SendKafkaMessage error: %s\n", err)
			}
			consumed++
		case <-signals:
			log.Println("Interrupted...")
			log.Printf("consumed %d messages\n", consumed)
			break ConsumerLoop
		}
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile)
	loglevel := logger.LevelInfo
	if *debug {
		loglevel = logger.LevelDebug
	}
	logger.SetLevel(loglevel)
	kafkaTopics := strings.Split(*kftopics, ",")

	var wg sync.WaitGroup
	wg.Add(len(kafkaTopics))

	for _, topic := range kafkaTopics {
		go func(topic string) {
			defer wg.Done()
			StartWorker(topic)
		}(topic)
	}

	wg.Wait()
}
