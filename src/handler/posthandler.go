package handler

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func WriteMessagewithPartition(partition int, key []byte, value []byte, logger *zap.Logger) int {
	conn, err := kafka.DialLeader(context.Background(), "tcp", viper.GetString("Brokers"), viper.GetString("Topic"), partition)
	if err == nil {
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, errt := conn.WriteMessages(
			kafka.Message{Key: key,
				Value: value},
		)
		if errt != nil {
			logger.Error(errt.Error())
			return 0
		}
		logger.Info("Message send to Partition", zap.Int("Partition", partition))
		conn.Close()
		return 3
	}
	logger.Error(err.Error())
	return 0

}
