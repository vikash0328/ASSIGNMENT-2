package main

import (
	"fmt"
     "encoding/json"
	"github.com/gin-gonic/gin"
	
	"github.com/segmentio/kafka-go"
	"context"
	"net/http"
)
 
 type Body struct {
	Email string `json:"Email"`
    Phone  string `json:"Phone"`
    MessageBody  string `json:"MessageBody"`
	Transactionid  string `json:"Transactionid"`
	Customerid    string   `json:"Custemerid"`
	Key       string       `json:"Key"`
}

func getkafkawriter() *kafka.Writer{
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "email",
		Balancer: &kafka.LeastBytes{},
	})

   return w
}
func writemessagewithkey( w *kafka.Writer, key[]byte, value []byte) string {
   err:=w.WriteMessages(context.Background(),
		kafka.Message{
			Key:key ,
			Value:value,
		},
	)
	if err==nil{
		fmt.Println("Succesfully Send")
	} else{
		fmt.Println(err.Error())
	}
	if(key==nil){
		return "message send without key"
	}else{
		return "message send with key"
	}
	
}

func handlepost(c *gin.Context){
	var jbody Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println(jbody)
	w:=getkafkawriter()
	
	 // Create JSON from the instance data.
	 // ... Ignore errors.
	 b, _ := json.Marshal(jbody)
	var s string
	 if(jbody.Key!=""){
	 s=writemessagewithkey(w,[]byte(jbody.Key),[]byte(string(b)))
	
	 }else{
		 s=writemessagewithkey(w,nil,[]byte(string(b)))
		 
	 }
	 fmt.Println(s)


	c.JSON(200, gin.H{"message": "HELLO WORLD!", "Name": "Swapnil Barai"})	
	w.Close()
}

func main() {

	
	
	
	
	r := gin.Default()

	r.POST("/",handlepost ) 
		
	r.Run()
	
   
	fmt.Println("hello world")
}
