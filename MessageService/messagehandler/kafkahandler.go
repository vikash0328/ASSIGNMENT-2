package messagehandler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

var logger *zap.Logger
var StateEmail [3]int

type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

func PassRefLog(log *zap.Logger) {
	logger = log
}
func startingFailure(j int, wg *sync.WaitGroup) {
	handleFailure(j)

	wg.Done()
}

func kakfareader() *kafka.Reader {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{viper.GetString("Brokers")},
		GroupID:        viper.GetString("GroupName"),
		Topic:          viper.GetString("Topic"),
		CommitInterval: 5 * time.Second,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})
	return r
}

func HandleMessagesParallel(j int, wg *sync.WaitGroup) {

	r := kakfareader()
	defer r.Close()
	u := true
	for {
		if u && (StateEmail[j] == 1) {
			handleFailure(j)
		}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			logger.Error(err.Error())
			break
		}
		var body Body
		json.Unmarshal(m.Value, &body)
		fmt.Println(m.Partition)
		logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
		logger.Info(string(m.Value))
		u = send(m.Value, j)

	}

	wg.Done()

}

func RecieveAndHandleMail() {
	var wg sync.WaitGroup

	wg.Add(3)
	for k := 0; k < 3; k++ {

		go startingFailure(k, &wg)

	}
	wg.Wait()

	StateEmail[0] = 0
	StateEmail[1] = 0
	StateEmail[2] = 0

	wg.Add(3)
	for i := 0; i < 3; i++ {

		go HandleMessagesParallel(i, &wg)

	}

	wg.Wait()

}
