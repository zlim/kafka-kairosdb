package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"github.com/Shopify/sarama"
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

// Datapoint instances are persisted back to kairosdb via AddDatapoints
type Datapoint struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     string            `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func KafkaMessageToDatapoint(m *sarama.ConsumerMessage) Datapoint {
	tags := make(map[string]string)
	key := string(m.Key)
	if len(key) == 0 {
		key = "NULL"
	}
	tags["key"] = key
	return Datapoint{
		Name:      m.Topic,
		Timestamp: time.Now().Unix(), //FIXME: use event-time instead of processing-time
		Value:     string(m.Value),
		Tags:      tags,
	}
}

func (kdb *Kairosdb) SendKafkaMessages(ms []*sarama.ConsumerMessage) error {
	datapoints := make([]Datapoint, len(ms))
	for i, m := range ms {
		datapoints[i] = KafkaMessageToDatapoint(m)
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
	log.Println(string(json))
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
		log.Println("consumer closed")
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
		log.Println("partitionConsumer closed")
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
			log.Println("Interrupted by user...")
			log.Printf("consumed %d messages\n", consumed)
			break ConsumerLoop
		}
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lshortfile)
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
