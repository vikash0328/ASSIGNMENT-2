package emailhandler

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var logger *zap.Logger

type ConsumerFailure struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Partition int                `bson:"partition,omitempty"`
	Offset    int64              `bson:"offset,omitempty"`
}

func PassRefLog(log *zap.Logger) {
	logger = log
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

func PartitionInsertIntial(partition int, offset int64) bool {

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

func UpdateOffset(partition int, offset int64) {
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

func Check(partition int, offset int64) int {
	client := connect()
	if client == nil {
		return 0
	}
	var b ConsumerFailure
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	filter := bson.M{"partition": partition}
	err := collection.FindOne(ctx, filter).Decode(&b)

	if err == nil && offset > b.Offset {
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

		if PartitionInsertIntial(partition, offset) == true {
			return 2
		} else {
			return 0
		}
	}

	return 1
}
