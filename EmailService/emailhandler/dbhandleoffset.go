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

//binding for mongo document for handling offset managment

type ConsumerFailure struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Partition int                `bson:"partition,omitempty"`
	Offset    int64              `bson:"offset,omitempty"`
}

//pasing ref of logger so that it can be used in emailhandler package

func PassRefLog(log *zap.Logger) {
	logger = log
}

//make connction to mongodb with context of 2 second
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

// handle case when new broker is added to consumer-group system and it increases the no. partition
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

// update the offset  of provided  partition when new message comes to partition
func UpdateOffset(j int, partition int, offset int64) {
	client := connect()
	if client == nil {
		logger.Warn("Their is Problem in Database Connction")
		OffsetFail[j] = true
		return
	}

	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	filter := bson.M{"partition": partition}
	//update := bson.M{"partition": partition, "offset": offset}
	update := bson.M{"$set": bson.M{"offset": offset, "partition": partition}}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	updateResult, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		logger.Fatal(err.Error())
		OffsetFail[j] = true
		return
	}
	OffsetFail[j] = false
	fmt.Println(updateResult)
	logger.Info("Upadated the count of Offset", zap.Int("Partition", partition), zap.Int64("Offset", offset))
}

// handle Intial case when consumer up or partition rebancing or new partition assignment
func Check(partition int, offset int64) int {
	client := connect()
	if client == nil {
		return 0
	}
	var b ConsumerFailure
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection1"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	filter := bson.M{"partition": partition}          //filter for checking latest offset for given partition
	err := collection.FindOne(ctx, filter).Decode(&b) // get the latest offset stored in database for given partition

	//check for the offset comming from database and kafka-broker group
	// if latest offset in database is less than latest offset from kafka-group then it will update it offset
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
		// condition when given partition is not present databse ,so insert it
		if PartitionInsertIntial(partition, offset) == true {
			return 2
		} else {
			return 0
		}
	}
	logger.Info("Stopped Duplicating message", zap.Int64("offset:", offset))
	return 1
}
