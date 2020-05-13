package main

import ( "fmt"
"github.com/segmentio/kafka-go"
	"context"
	"encoding/json"
)

type Body struct {
    Email string
    Phone  string
    MessageBody  string
	Transactionid  string
	Customerid    string
	Key       string
}

func kakfareader() *kafka.Reader{
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "email",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
    return r
}



func main(){

	
   r:=kakfareader()
	
	

	for {
		m, err := r.ReadMessage(context.Background())
		
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		var body Body
		json.Unmarshal(m.Value,&body)
		fmt.Printf("message at topic/partition/offset %v/%v/%v n", m.Topic ,string(m.Key) ,m.Offset)
		fmt.Println(body)
		
	}
	
	r.Close()


}