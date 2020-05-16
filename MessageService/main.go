package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger
var state_email int

type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email ,omitempty" json:"Email ,omitempty"`
	Phone         string             `bson:"Phone ,omitempty" json:"Phone ,omitempty"`
	MessageBody   string             `bson:"MessageBody, omitempty" json:"MessageBody, omitempty"`
	Transactionid string             `bson:"Transactionid, omitempty" json:"Transactionid, omitempty"`
	Customerid    string             `bson:"Customerid, omitempty" json:"Customerid, omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

func connect() *mongo.Client {
	/*credential := options.Credential{
		Username: "swapnil",
		Password: "swapnil@123",
	}*/
	//clientOpts := options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(credential)
	clientOpts := options.Client().ApplyURI("mongodb://localhost:27017")
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return client

}

func handleInsert(data []byte) {
	var b Body
	json.Unmarshal(data, &b)
	client := connect()
	if client == nil {
		return
	}
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	result, er := collection.InsertOne(ctx, b)
	if er != nil {
		logger.Error(er.Error())
	} else {
		logger.Info("Succesfully Inserted")
		fmt.Println(result)
	}

}

func handleFailure() {
	client := connect()
	if client == nil {
		return
	}
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	cursor, er := collection.Find(ctx, bson.M{})
	if er != nil {
		logger.Error(er.Error())
		return
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var b Body
		cursor.Decode(&b)
		d, _ := json.Marshal(b)
		if send([]byte(d)) {
			ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
			res, erd := collection.DeleteOne(ctm, bson.M{"_id": b.ID})
			if erd != nil {
				logger.Error(erd.Error())
				return
			}
			logger.Info("Succesfully Delete")
			logger.Info("Successfully Send", zap.String("Transaction_id", b.Transactionid))
			fmt.Println(res)
		} else {
			logger.Info("Email Sevice is down")
			return
		}

	}
	state_email = -1
}

func kakfareader() *kafka.Reader {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{viper.GetString("Brokers")},
		Topic:    viper.GetString("Topic"),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return r
}

func initZapLog() *zap.Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"mess.log"},
		ErrorOutputPaths: []string{"mess.log"},
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

func send(m []byte) bool {
	var body Body
	json.Unmarshal(m, &body)

	from := "swapnilbarai149@gmail.com"
	pass := "......."
	to := body.Phone + "@sms.clicksend.com" // receivers mobile number +N number  format
	msg := "Your Trnsaction is Completed:" + "\n" + "Transaction_id: " + string(body.Transactionid) + "\n" + "Customer_id: " + string(body.Customerid) + "\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		logger.Error(err.Error())
		handleInsert(m)
		state_email = 1
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}

func main() {

	ftmh := 0
	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		logger.Error(err.Error())
	}

	r := kakfareader()
	logger = initZapLog()

	u := true
	for {
		if u && (state_email == 0 || state_email == 1 || ftmh == 0) {
			handleFailure()
		}
		m, err := r.ReadMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}
		var body Body
		json.Unmarshal(m.Value, &body)
		logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
		logger.Info(string(m.Value))
		send(m.Value)

		u = send(m.Value)
		ftmh = 1
	}
	logger.Sync()
	r.Close()

}
