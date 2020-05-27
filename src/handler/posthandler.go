package handler

import (
	"context"
	"time"
	"math/rand"
	"strings"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

/***************************************************************************************************
*  this function take partition no. to which the message to send, key in bytes ,messages to send in bytes
*  logger as pointer for writing log
*  first its sets configuration using kafka.DialLeader
*  then its sets deadline for sending message using SetWriteDeadline method
*  finally ,it  send message with  conn.WriteMessages  method
*  con..WriteMessages method return no. bytes it sends if sussesfull else return error
*  return values are helpful post api response
******************************************************************************************************/

func WriteMessagewithPartition(partition int, key []byte, value []byte, logger *zap.Logger) int {
	x := rand.Intn(3)
	a := strings.Split(viper.GetString("Brokers"), ",")[x]
	fmt.Println(a)
	conn, err := kafka.DialLeader(context.Background(), "tcp", a, viper.GetString("Topic"), partition)
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
