package main

import (
	"fmt"
     "encoding/json"
	"github.com/gin-gonic/gin"
	
	"github.com/segmentio/kafka-go"
	"context"
	"net/http"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap"
	
)
 
var logger *zap.Logger



 type Body struct {
	Email string `json:"Email"`
    Phone  string `json:"Phone"`
    MessageBody  string `json:"MessageBody"`
	Transactionid  string `json:"Transactionid"`
	Customerid    string   `json:"Custemerid"`
	Key       string       `json:"Key"`
}

func getkafkawriter() *kafka.Writer{
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "email",
		Balancer: &kafka.LeastBytes{},
	})

   return w
}

func initZapLog() *zap.Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    "time",
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	logger, _ = cfg.Build()
    return logger
}



func writemessagewithkey( w *kafka.Writer, key[]byte, value []byte) int{
   err:=w.WriteMessages(context.Background(),
		kafka.Message{
			Key:key ,
			Value:value,
		},
	)
	if err==nil{
		if(key!=nil){
		logger.Info("Message Successfully Send" , zap.String("key",string(key)))
		}else{
			logger.Info("Meassage send succesfully without key")
		}
       return 1
	} 
		logger.Error(err.Error())
		return 0
	
	
	
}



func handlepost(c *gin.Context){
	var jbody Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println(jbody)
	w:=getkafkawriter()
	
	 
	 b, _ := json.Marshal(jbody)
	var s int
	 if(jbody.Key!=""){
	 s=writemessagewithkey(w,[]byte(jbody.Key),[]byte(string(b)))
	
	 }else{
		 s=writemessagewithkey(w,nil,[]byte(string(b)))
		 
	 }
	 if(s==0){
	c.JSON(200, gin.H{"message": "Error", "Body": "Couldn't complete your request"})	
	 } else{
		 c.JSON(200,gin.H{"message":"Success","Body":"your Transaction is Completed"})
	 }
	w.Close()
}

func main() {

	logger=initZapLog()
	
	
	
	r := gin.Default()

	r.POST("/",handlepost ) 
		
	r.Run()
	
	 logger.Sync()

}
