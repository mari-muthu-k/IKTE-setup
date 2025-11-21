package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MessagesPerProducer = 10_000_000
	RateLimit           = 130 // messages per second
	Topic               = "ad_pii_topic"
)

type AdPII struct {
	RecordID    int       `json:"record_id"`
	AdID        string    `json:"advertising_id"`
	CookieID    string    `json:"cookie_id"`
	DeviceID    string    `json:"device_id"`
	HouseholdID string    `json:"household_id"`
	Segment     string    `json:"segment"`
	Timestamp   time.Time `json:"timestamp"`
}

func generatePII(id int) AdPII {
	return AdPII{
		RecordID:    id,
		AdID:        uuid.New().String(),
		CookieID:    uuid.New().String(),
		DeviceID:    uuid.New().String(),
		HouseholdID: fmt.Sprintf("HH-%d", rand.Intn(99999)),
		Segment:     fmt.Sprintf("segment-%d", rand.Intn(50)),
		Timestamp:   time.Now(),
	}
}

var producedCounter = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "producer_messages_total",
		Help: "Total messages produced",
	},
)

func init() {
	prometheus.MustRegister(producedCounter)
}

func main() {

	broker := os.Getenv("KAFKA_BROKER_URL")
	if broker == "" {
		broker = "kafka:9092"
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionLZ4
	config.Producer.Flush.Frequency = 5 * time.Millisecond

	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	fmt.Println("Producer started with limit:", RateLimit, "msg/sec")

	ticker := time.NewTicker(time.Second / RateLimit)

	for i := 0; i < MessagesPerProducer; i++ {

		<-ticker.C // enforce rate limit

		msgData, _ := json.Marshal(generatePII(i))

		producer.Input() <- &sarama.ProducerMessage{
			Topic: Topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", i)),
			Value: sarama.ByteEncoder(msgData),
		}

		fmt.Println("Published :", i)
		producedCounter.Inc()
	}

	fmt.Println("✅ Published 10 million messages successfully")
}
