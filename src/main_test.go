package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"swap/src/handler"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/assert/v2"
)

func postResponse(post handler.Body, t *testing.T) string {

	b, _ := json.Marshal(post)

	r := gin.Default()
	var request *http.Request
	if post.Partition == -1 {
		r.POST("/home", handler.Handlepost)
		req, _ := http.NewRequest("POST", "/home", strings.NewReader(string(b)))
		request = req

	} else {

		path := "/home/:Partition"
		partition := strconv.Itoa(post.Partition)
		fmt.Println(partition)
		//path := "/home/" + partition + "/"
		//context.Set(r, 0, map[string]string{"Partition": partition})
		r.POST(path, handler.HandlepostPartition)
		req, _ := http.NewRequest("POST", path, strings.NewReader(string(b)))
		request = req
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)

	assert.Equal(t, 200, w.Code)
	y := w.Body.String()
	return y
}

func TestApiResponse(t *testing.T) {

	if !handler.InitViper() {
		t.Error("Unable to open Viper file")
	}
	//w := Getkafkawriter()
	//	defer w.Close()

	logger = handler.InitZapLog()
	defer logger.Sync()
	handler.PassRefLog(&(*logger))
	var body [4]string

	body[0] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message sent with Key","message":"Success"}`))

	//use body[0] when message is sent and key is provided without partition

	//use body[1]for testing when message is sent without key and without partition

	body[1] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message sent without Key","message":"Success"}`))

	//use body [2] for testing when nessage is sent with given partition number

	body[2] = string([]byte(`{"Body":"your Transaction is Completed","Info":"Message has been sent to provided Partition","message":"Success"}`))

	body[3] = string([]byte(`{"Body":"Couldn't complete your request","message":"Error"}`))
	//use body[3] when your broker is not working

	post := handler.Body{
		Email:         "vikasharya0328@gmail.com",
		Phone:         "7432094921",
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
		//post.Partition = 0
		y := postResponse(post, t)

		// Make some assertions on the correctness of the response.

		assert.Equal(t, body[i], y)

		//test case when message connected to broker

	}

}
