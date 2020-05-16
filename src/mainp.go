package main

import (
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"

	"context"
	"net/http"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

type Body struct {
	Email         string `json:"Email"`
	Phone         string `json:"Phone"`
	MessageBody   string `json:"MessageBody"`
	Transactionid string `json:"Transactionid"`
	Customerid    string `json:"Custemerid"`
	Key           string `json:"Key"`
}

func getkafkawriter() *kafka.Writer {
	// viper.SetConfigName("config")

	// viper.AddConfigPath(".")

	// viper.AutomaticEnv()

	// viper.SetConfigType("yml")

	// if err := viper.ReadInConfig(); err != nil {
	// 	logger.Error(err.Error())
	// }
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{viper.GetString("Brokers")},
		Topic:    viper.GetString("Topic"),
		Balancer: &kafka.LeastBytes{},
	})

	return w
}

func initZapLog() *zap.Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"prod.log"},
		ErrorOutputPaths: []string{"prod.log"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: viper.GetString("MessageKey"),

			LevelKey:    viper.GetString("LevelKey"),
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    viper.GetString("TimeKey"),
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    viper.GetString("CallerKey"),
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	logger, _ = cfg.Build()
	return logger
}

func writemessagewithkey(w *kafka.Writer, key []byte, value []byte) int {
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)
	if err == nil {
		if key != nil {
			logger.Info("Message Successfully Send", zap.String("key", string(key)))
		} else {
			logger.Info("Meassage send succesfully without key")
		}
		return 1
	}
	logger.Error(err.Error())
	return 0

}

func handlepost(c *gin.Context) {
	var jbody Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println(jbody)
	w := getkafkawriter()

	b, _ := json.Marshal(jbody)
	var s int
	if jbody.Key != "" {
		s = writemessagewithkey(w, []byte(jbody.Key), []byte(string(b)))

	} else {
		s = writemessagewithkey(w, nil, []byte(string(b)))

	}
	if s == 0 {
		c.JSON(200, gin.H{"message": "Error", "Body": "Couldn't complete your request"})
	} else {
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed"})
	}
	w.Close()
}

func main() {

	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		logger.Error(err.Error())
	}

	logger = initZapLog()

	r := gin.Default()

	r.POST("/", handlepost)

	r.Run()

	logger.Sync()

}
