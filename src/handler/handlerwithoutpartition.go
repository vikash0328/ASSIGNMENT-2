package handler

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

/****************************************************************************************
getKafkawriter Get configuration from Viper File and
return *kafka.Writer instance for sending message to broker
It will take paramter like Brokerslist Topic
Balancer is used to detrmine to which Partition message will be send

we are using kafka.LeastBytes which send the message to  partition which recievied least
bytes of messages
********************************************************************************************/

func Getkafkawriter() *kafka.Writer {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{viper.GetString("Brokers")},
		Topic:    viper.GetString("Topic"),
		Balancer: &kafka.LeastBytes{},
	})

	return w
}

func Writemessagewithkey(w *kafka.Writer, key []byte, value []byte, logger *zap.Logger) int {
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)
	if err == nil {
		if key != nil {
			logger.Info("Message Successfully Send", zap.String("key", string(key)))
			return 1
		} else {
			logger.Info("Meassage send succesfully without key")
			return 2
		}

	}
	logger.Error(err.Error())
	return 0

}
