package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"strings"

	"sync"
	"time"

	"github.com/natefinch/lumberjack"
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
var state_email [3]int

type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

func initZapLog() *zap.Logger {
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "mess.log",
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
		logger.Error(err.Error())
		return nil
	}
	return client

}

func handleInsert(data []byte, j int) {
	var b Body
	json.Unmarshal(data, &b)
	client := connect()
	if client == nil {
		return
	}
	collectionName := strings.Split(viper.GetString("collection"), ",")
	collection := client.Database(viper.GetString("database")).Collection(collectionName[j])

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	result, er := collection.InsertOne(ctx, b)
	if er != nil {
		logger.Error(er.Error())
	} else {
		logger.Info("Succesfully Inserted")
		fmt.Println(result)
	}

}

func handleFailure(j int) bool {
	client := connect()
	if client == nil {
		return false
	}

	collectionName := strings.Split(viper.GetString("collection"), ",")
	collection := client.Database(viper.GetString("database")).Collection(collectionName[j])

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	cursor, er := collection.Find(ctx, bson.M{})
	if er != nil {
		logger.Error(er.Error())
		return false
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var b Body
		cursor.Decode(&b)
		d, _ := json.Marshal(b)
		if send([]byte(d), j) {
			ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
			res, erd := collection.DeleteOne(ctm, bson.M{"_id": b.ID})
			if erd != nil {
				logger.Error(erd.Error())
				return false
			}
			logger.Info("Succesfully Delete")
			logger.Info("Successfully Send", zap.String("Transaction_id", b.Transactionid))
			fmt.Println(res)
		} else {
			logger.Info("Email Sevice is down")
			return false
		}

	}
	state_email[j] = -1
	return true
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

/*

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
*/

func send(m []byte, j int) bool {
	var body Body
	json.Unmarshal(m, &body)

	from := "swapnil.bro123@gmail.com"
	pass := "Let@123#rt"
	to := body.Phone + "@sms.clicksend.com" // receivers mobile number +N number  format
	msg := "Your Trnsaction is Completed:" + "\n" + "Transaction_id: " + string(body.Transactionid) + "\n" + "Customer_id: " + string(body.Customerid) + "\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		logger.Error(err.Error())
		handleInsert(m, j)
		state_email[j] = 1
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}

func main() {
	var wg sync.WaitGroup

	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err.Error())
		//logger.Error(err.Error())
	}
	logger = initZapLog()
	wg.Add(3)
	for k := 0; k < 3; k++ {

		go func(j int) {
			handleFailure(j)

			wg.Done()
		}(k)
	}
	wg.Wait()

	state_email[0] = 0
	state_email[1] = 0
	state_email[2] = 0

	wg.Add(3)
	for i := 0; i < 3; i++ {

		go func(j int) {
			r := kakfareader()
			u := true
			for {
				if u && (state_email[j] == 1) {
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
			r.Close()
			wg.Done()
		}(i)
	}
	logger.Sync()

	wg.Wait()

}
