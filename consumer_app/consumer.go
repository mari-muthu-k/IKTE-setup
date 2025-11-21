package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	BatchSize       = 10000
	SleepAfterBatch = 30 * time.Second
	Topic           = "ad_pii_topic"
)

type AdPII struct {
	RecordID int    `json:"record_id"`
	AdID     string `json:"advertising_id"`
}

var batchCounter = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "consumer_batches_total",
		Help: "Number of batches processed",
	},
)
var batchLatency = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name: "consumer_batch_duration_seconds",
		Help: "Batch processing + sleep duration",
	},
)

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2113", nil)
	}()

	broker := os.Getenv("KAFKA_BROKER_URL")
	if broker == "" {
		broker = "kafka:9092"
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{broker}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(Topic)
	if err != nil {
		panic(err)
	}

	fmt.Println("🚀 Consumer started with batch size:", BatchSize)

	for _, partition := range partitions {
		go func(part int32) {

			pc, err := consumer.ConsumePartition(Topic, part, sarama.OffsetOldest)
			if err != nil {
				panic(err)
			}
			defer pc.Close()

			batch := make([]AdPII, 0, BatchSize)

			for msg := range pc.Messages() {

				var record AdPII
				json.Unmarshal(msg.Value, &record)
				batch = append(batch, record)

				if len(batch) >= BatchSize {
					fmt.Printf("🟢 Partition %d processed batch of %d\n", part, len(batch))
					batch = batch[:0]

					batchCounter.Inc()
					start := time.Now()

					time.Sleep(SleepAfterBatch)
					batchLatency.Observe(time.Since(start).Seconds())
				}
			}

		}(partition)
	}

	select {}
}
