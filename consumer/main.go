package main

import ( 
"github.com/segmentio/kafka-go"
	"context"
	"encoding/json"
	"log"
	"net/smtp"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap"
)

var logger *zap.Logger


type Body struct {
    Email string
    Phone  string
    MessageBody  string
	Transactionid  string
	Customerid    string
	Key       string
}

func kakfareader() *kafka.Reader{
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "email",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
    return r
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


func send(m []byte) {
	var body Body
	json.Unmarshal(m,&body)

	from := "swapnilbarai149@gmail.com"
	pass := "......."
	to := body.Email

	msg := "Your Trnsaction is Completed: " + from + "\n" +
		"To: " + to + "\n" +
		"Subject:Transaction\n\n" +
		"Transaction_id: "+string(body.Transactionid)+"\n"+
		"Customer_id: "+string(body.Customerid)+"\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		log.Printf("smtp error: %s", err)
		return
	}
	
	logger.Info("Successfully Send",zap.String("Transaction_id",body.Transactionid))
}

func main(){

	
   r:=kakfareader()
   logger=initZapLog()
	
	
      
	for {
		m, err := r.ReadMessage(context.Background())
		
		if err != nil {
			logger.Error(err.Error())
			break
		}
		var body Body
		json.Unmarshal(m.Value,&body)
		logger.Info("metadata", zap.String("Topic",m.Topic) ,zap.String("Key",string(m.Key) ),zap.Int64("Offset",m.Offset))
		logger.Info(string(m.Value))
		send(m.Value)
		
	}
	logger.Sync()
	r.Close()


}