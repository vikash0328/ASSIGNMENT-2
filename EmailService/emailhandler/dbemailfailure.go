package emailhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

/*func connect() *mongo.Client {
/*credential := options.Credential{
	Username: "swapnil",
	Password: "swapnil@123",
}*/
//clientOpts := options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(credential)
/*clientOpts := options.Client().ApplyURI("mongodb://localhost:27017")
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return client

}*/

var State_email int

func HandleInsert(data []byte) {
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

func HandleFailure() int {
	client := connect()
	if client == nil {
		return 0
	}
	collection := client.Database(viper.GetString("database")).Collection(viper.GetString("collection"))

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	cursor, er := collection.Find(ctx, bson.M{})
	if er != nil {
		logger.Error(er.Error())
		return 0
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var b Body
		cursor.Decode(&b)
		d, _ := json.Marshal(b)
		if Send([]byte(d)) {
			ctm, _ := context.WithTimeout(context.Background(), 2*time.Second)
			res, erd := collection.DeleteOne(ctm, bson.M{"_id": b.ID})
			if erd != nil {
				logger.Error(erd.Error())
				return 0
			}
			logger.Info("Succesfully Delete")
			logger.Info("Successfully Send", zap.String("Transaction_id", b.Transactionid))
			fmt.Println(res)
		} else {
			logger.Info("Email Sevice is down")
			return 0
		}

	}
	return -1
}
func Send(m []byte) bool {
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
		HandleInsert(m)
		State_email = 1
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}
