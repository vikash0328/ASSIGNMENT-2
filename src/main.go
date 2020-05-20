package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"

	"context"
	"net/http"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

type Body struct {
	Email         string `json:"Email"`
	Phone         string `json:"Phone"`
	MessageBody   string `json:"MessageBody"`
	Transactionid string `json:"Transactionid"`
	Customerid    string `json:"Customerid"`
	Key           string `json:"Key"`
	Partition     int    `json:"partition"`
}

/******************************************************************************************************
this is syncronous send method in function writeMessagewithPartition
con object return bytes  if succesful else it return err
con takes partition no. brokers address ad topic as well as
protocal of sending message

***********************************************************************************************************/

func writeMessagewithPartition(partition int, key []byte, value []byte) int {
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

/***************************************************************************************
getKafkawriter Get configuration from Viper File and
return *kafka.Writer instance for sending message to broker
It will take paramter like Brokerslist Topic
Balancer is used to detrmine to which Partition message will be send

we are using kafka.LeastBytes which send the message to  partition which recievied least
bytes of messages
********************************************************************************************/

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



/*************************************************************************************************

initZapLog will intialise properties fo logger.





****************************************************************************************************/

func initZapLog() *zap.Logger {
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "prod.log",
		MaxSize:    1, // megabytes
		MaxBackups: 30,
		MaxAge:     30, // days
	})

	config := zapcore.EncoderConfig{
		MessageKey: viper.GetString("MessageKey"),

		LevelKey:    viper.GetString("LevelKey"),
		EncodeLevel: zapcore.CapitalLevelEncoder,

		TimeKey:    viper.GetString("TimeKey"),
		EncodeTime: zapcore.ISO8601TimeEncoder,

		CallerKey:    viper.GetString("CallerKey"),
		EncodeCaller: zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config),
		zapcore.NewMultiWriteSyncer(w),
		zap.InfoLevel,
	)
	logger := zap.New(core, zap.AddCaller(), zap.Development())
	logger.Sugar()
	return logger
}

/*****************************************************************************************





*********************************************************************************************/

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
			return 1
		} else {
			logger.Info("Meassage send succesfully without key")
			return 2
		}

	}
	logger.Error(err.Error())
	return 0

}

func Handlepost(c *gin.Context) {
	var jbody Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	//logger = initZapLog()
	fmt.Println(jbody)
	

	b, _ := json.Marshal(jbody)
	fmt.Println(jbody)
	var s int
	if jbody.Partition == -1 {
		w := getkafkawriter()
		if jbody.Key != "" {
			s = writemessagewithkey(w, []byte(jbody.Key), []byte(string(b)))

		} else {
			s = writemessagewithkey(w, nil, []byte(string(b)))

		}
		w.Close()
	} else {

		if jbody.Key != "" {
			s = writeMessagewithPartition(jbody.Partition, []byte(jbody.Key), []byte(string(b)))

		} else {
			s = writeMessagewithPartition(jbody.Partition, nil, []byte(string(b)))

		}
	}
	if s == 0 {
		c.JSON(200, gin.H{"message": "Error", "Body": "Couldn't complete your request"})
	} else if s == 1 {
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message sent with Key"})
	} else if s == 2 {
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message sent without Key"})
	} else {
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message has been sent to provided Partition"})
	}
	
}

func main() {

	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err.Error())
	}

	logger = initZapLog()

	r := gin.Default()

	r.POST("/", Handlepost)

	r.Run()

	logger.Sync()

}
