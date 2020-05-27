package emailhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

// binding for message comes from kafka-consumer group

type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

//case when mail server is down so it will insert given message in database

func HandleInsert(data []byte, j int) bool {
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

func HandleFailure(j int) bool {
	client := connect()
	if client == nil {
		return false
	}
	collectionName := strings.Split(viper.GetString("collection"), ",")
	collection := client.Database(viper.GetString("database")).Collection(collectionName[j])

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	cursor, er := collection.Find(ctx, bson.M{}) //find all messages in database to send
	if er != nil {
		logger.Error(er.Error())
		return false
	}
	defer cursor.Close(ctx)
	//iterating over all messages
	for cursor.Next(ctx) {
		var b Body
		cursor.Decode(&b)
		d, _ := json.Marshal(b)
		//send message return true for success
		if Send([]byte(d), j) {
			ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
			res, erd := collection.DeleteOne(ctm, bson.M{"_id": b.ID}) //delete the message which are sucessfully send
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
	StateEmail[j] = -1
	return true //indicating that all messages have deleted from database
}

// send message to provided email-id

func Send(m []byte, j int) bool {
	var body Body
	//binding for converting byte data struct type Body
	json.Unmarshal(m, &body)

	from := "..."
	pass := "..."
	to := body.Email
	fmt.Println(body)
	msg := "Your Transaction is Completed: " + from + "\n" +
		"To: " + to + "\n" +
		"Subject:Transaction\n\n" +
		"Transaction_id: " + string(body.Transactionid) + "\n" +
		"Customer_id: " + string(body.Customerid) + "\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		logger.Error(err.Error())
		if StopInsert[j] == true {
			logger.Warn("Successfully stop Duplicating Message in datbase")
			return false
		}
		//case when email server id down so insert data into database
		if HandleInsert(m, j) {
			StateEmail[j] = 1 //it indicate that their is message in database to send
			return false
		}
		DBEmailFail[j] = true
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}
