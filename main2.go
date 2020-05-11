package main

import (
	"fmt"
     "encoding/json"
	"github.com/gin-gonic/gin"
	
	"github.com/segmentio/kafka-go"
	"context"
)
 
 type Body struct {
    Email string
    Phone  string
    MessageBody  string
	Transactionid  string
	Customerid    string
	Key       string
}

func getkafkawriter() *kafka.Writer{
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "email",
		Balancer: &kafka.LeastBytes{},
	})

   return w
}

func main() {

	body := Body{
	   Email:"swapnilbarai149@gmail.com",
	   Phone:"9834034808",
	   MessageBody:"Your Transaction is completed",
	   Transactionid:"123456",
	   Customerid:"3456789",
	   Key:"1234567",
    }
    // Create JSON from the instance data.
    // ... Ignore errors.
    c, _ := json.Marshal(body)
    // Convert bytes to string.
    d := string(c)
    fmt.Println(d)
	
	w:=getkafkawriter()
	err:=w.WriteMessages(context.Background(),
		kafka.Message{
			Key: []byte("body.Key"),
			Value:[]byte(c),
		},
	)
	if err==nil{
		fmt.Println("done")
	} else{
		fmt.Println("sucessfully Send")
	}
	w.Close()
	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		
		c.JSON(500, gin.H{"message": "HELLO WORLD!", "Name": "Swapnil Barai"})
	})
	r.Run()
	

	fmt.Println("hello world")
}
