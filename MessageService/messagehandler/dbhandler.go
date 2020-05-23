package messagehandler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

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

//case when mail server is down so it will insert given message in database
func handleInsert(data []byte, j int) bool {
	var b Body
	json.Unmarshal(data, &b)
	client := connect()
	if client == nil {
		return false
	}
	collectionName := strings.Split(viper.GetString("collection"), ",")
	collection := client.Database(viper.GetString("database")).Collection(collectionName[j])

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	result, er := collection.InsertOne(ctx, b)
	if er != nil {
		logger.Error(er.Error())
		return false
	} else {
		logger.Info("Succesfully Inserted")
		fmt.Println(result)
	}
	return true
}

func handleFailure(j int) bool {
	client := connect()
	if client == nil {
		return false
	}

	collectionName := strings.Split(viper.GetString("collection"), ",")
	collection := client.Database(viper.GetString("database")).Collection(collectionName[j])

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	cursor, er := collection.Find(ctx, bson.M{}) //find all messages in database for jth gorotine to send
	if er != nil {
		logger.Error(er.Error())
		return false
	}
	defer cursor.Close(ctx)
	//itrating over all messages for jth goroutine
	for cursor.Next(ctx) {
		var b Body
		cursor.Decode(&b)
		d, _ := json.Marshal(b)
		//send message return true for success for jth goroutine
		if send([]byte(d), j) {
			ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
			res, erd := collection.DeleteOne(ctm, bson.M{"_id": b.ID})
			//error means database is down so return
			if erd != nil {
				logger.Error(erd.Error())
				return false
			}
			logger.Info("Succesfully Delete")
			logger.Info("Successfully Send", zap.String("Transaction_id", b.Transactionid))
			fmt.Println(res)
		} else {
			//error means database is down so return
			logger.Info("Email Sevice is down")
			return false
		}

	}
	StateEmail[j] = -1 //indicating that all messages have deleted from database-collection jth goroutine
	return true
}
