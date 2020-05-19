package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"sync"
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
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody, omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid, omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid, omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

type ConsumerFailure struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	partition int                `bson:"partition",omitempty`
	offset    int64              `bson:"offset",omitempty`
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

func partitionInsertIntial(partition int, offset int64) bool {

	client := connect()
	if client == nil {
		return false
	}

	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	result, er := collection.InsertOne(ctx, bson.M{"offset": offset, "partition": partition})
	if er != nil {
		logger.Error(er.Error())
		return false
	} else {
		fmt.Println(result)
	}
	return true
}

func updateOffset(partition int, offset int64) {
	client := connect()
	if client == nil {
		logger.Warn("Their is Problem in Database Connction")
	}

	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	filter := bson.M{"partition": partition}
	//update := bson.M{"partition": partition, "offset": offset}
	update := bson.M{"$set": bson.M{"offset": offset, "partition": partition}}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	updateResult, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		logger.Fatal(err.Error())
	}
	fmt.Println(updateResult)
	logger.Info("Upadated the count of Offset", zap.Int("Partition", partition), zap.Int64("Offset", offset))
}

func check(partition int, offset int64) int {
	client := connect()
	if client == nil {
		return 0
	}
	var b ConsumerFailure
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	filter := bson.M{"partition": partition}
	err := collection.FindOne(ctx, filter).Decode(&b)
	if err == nil && offset > b.offset {
		update := bson.M{"$set": bson.M{"offset": offset, "partition": partition}}

		ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
		updateResult, errt := collection.UpdateOne(ctm, bson.M{"_id": b.ID}, update)
		if errt != nil {
			logger.Fatal(err.Error())
			return 0
		}
		fmt.Println(updateResult)
		logger.Info("Upadated the count of Offset")
		return 3
	} else if err != nil {
		if partitionInsertIntial(partition, offset) == true {
			return 2
		} else {
			return 0
		}
	}

	return 1
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
		Brokers:        []string{viper.GetString("Brokers")},
		GroupID:        viper.GetString("GroupName"),
		Topic:          viper.GetString("Topic"),
		CommitInterval: 5 * time.Second,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})
	return r
}

func initZapLog() *zap.Logger {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"email.log"},
		ErrorOutputPaths: []string{"email.log"},
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

	from := "swapnil.bro123@gmail.com"
	pass := "Let@123#rt"
	to := body.Email
	fmt.Println(body)
	msg := "Your Trnsaction is Completed: " + from + "\n" +
		"To: " + to + "\n" +
		"Subject:Transaction\n\n" +
		"Transaction_id: " + string(body.Transactionid) + "\n" +
		"Customer_id: " + string(body.Customerid) + "\n"

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
	var wg sync.WaitGroup
	state_email = 0
	var fsm int64 = 1
	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		logger.Error(err.Error())
	}

	logger = initZapLog()
	r := kakfareader()
	PrevPartition := -1
	u := true
	for {
		if u && (state_email == 1 || fsm == 1) {
			handleFailure()
		}
		m, err := r.FetchMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}
		fmt.Println(fsm)

		if fsm == 1 || PrevPartition != m.Partition {
			PrevPartition = m.Partition

			t := check(m.Partition, m.Offset)
			if t == 1 {
				logger.Info("SuccesFully Stop From Duplicating Message")
				continue

			} else if t == 0 {
				logger.Warn("You Might Get Duplicate Messages .Their is Problem in DataBase Connection")
			} else if t == 2 {
				logger.Info("Your New Partion SuccesFully Entered", zap.Int("NewPartion:", m.Partition))
			}

		}
		wg.Add(2)
		func() {
			if fsm != 1 && PrevPartition == m.Partition {
				updateOffset(m.Partition, m.Offset)

			}
			wg.Done()
		}()
		func() {
			var body Body
			json.Unmarshal(m.Value, &body)
			logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
			logger.Info(string(m.Value))

			u = send(m.Value)
			wg.Done()
		}()
		wg.Wait()
		fsm = fsm + 1
		if fsm%8 == 0 {
			r.CommitMessages(context.Background(), m)
		}

	}
	logger.Sync()
	r.Close()

}
