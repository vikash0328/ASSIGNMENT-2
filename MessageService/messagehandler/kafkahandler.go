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

// binding for message comes from kafka-consumer group
type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

//pass ref of log of main so that it canbe used for logging purpose

func PassRefLog(log *zap.Logger) {
	logger = log
}

// intial case when consumer starts
func startingFailure(j int, wg *sync.WaitGroup) {
	//send and delete messages in database if email-sever is up
	handleFailure(j)

	wg.Done()
}

//reading configuration for joining particular group and topic
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

//handle message comming from goroutine
func HandleMessagesParallel(j int, wg *sync.WaitGroup) {

	r := kakfareader()
	defer r.Close()
	u := true
	for {
		if u && (StateEmail[j] == 1) {
			handleFailure(j)
			//situation when their is message in databse in the collection for jth goroutine and email service is up
		}
		m, err := r.ReadMessage(context.Background()) //getting messages from kafka-consumer group
		if err != nil {
			logger.Error(err.Error())
			break
		}
		var body Body
		json.Unmarshal(m.Value, &body)
		fmt.Println(m.Partition)
		logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
		logger.Info(string(m.Value))
		u = send(m.Value, j) // sending  message to message server

	}

	wg.Done()

}

func RecieveAndHandleMail() {
	var wg sync.WaitGroup

	wg.Add(3) //adding 3 go routine for 3 partition
	for k := 0; k < 3; k++ {

		go startingFailure(k, &wg)
		//senario when consumer is up

	}
	wg.Wait()

	StateEmail[0] = 0
	StateEmail[1] = 0
	StateEmail[2] = 0

	wg.Add(3) //adding 3 go routine for 3 partition
	for i := 0; i < 3; i++ {

		go HandleMessagesParallel(i, &wg)

	}

	wg.Wait()

}
