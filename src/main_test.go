package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/assert/v2"
	"github.com/spf13/viper"
)

func postResponse(post Body, t *testing.T) string {

	logger = initZapLog()

	b, _ := json.Marshal(post)

	r := gin.Default()

	r.POST("/", Handlepost)
	req, _ := http.NewRequest("POST", "/", strings.NewReader(string(b)))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	y := w.Body.String()
	return y
}

func TestApiResponse(t *testing.T) {

	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err.Error())
	}

	var body [4]string

	body[0] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message sent with Key","message":"Success"}`))

	//use body[0] when message is sent and key is provided without partition

	//use body[1]for testing when message is sent without key and without partition

	body[1] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message sent without Key","message":"Success"}`))

	//use body [2] for testing when nessage is sent with given partition number

	body[2] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message has been sent to provided Partition","message":"Success"}`))

	body[3] = string([]byte(`{"Body":"Couldn't complete your request","message":"Error"}`))
	//use body when your broker is not working

	post := Body{
		Email:         "sunilsn@iitk.ac.in",
		Phone:         "9916194654",
		MessageBody:   "hellohiytttty",
		Transactionid: "125456456",
		Customerid:    "347656r76789",
		Key:           "14244",
		Partition:     -1}

	/*	y := postResponse(post, t)
		assert.Equal(t, body[3], y)
	*/

	// change key ,partition for testing of body

	for i := 0; i < 3; i++ {

		if i == 0 {
			post.Partition = -1

		} else if i == 1 {
			post.Key = ""
		} else if i == 2 {
			post.Partition = 0
		}
		y := postResponse(post, t)

		// Make some assertions on the correctness of the response.

		assert.Equal(t, body[i], y)

		//test case when message connected to broker

	}

}
