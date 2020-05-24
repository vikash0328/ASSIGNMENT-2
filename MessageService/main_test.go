package main

import (
	"encoding/json"
	"testing"

	"github.com/go-playground/assert/v2"

	"swap/MessageService/messagehandler"
)

func Test_main(t *testing.T) {

	y := messagehandler.InitVip()
	assert.Equal(t, true, y)

	logger = messagehandler.InitZapLog()
	messagehandler.PassRefLog(&(*logger))
	defer logger.Sync()

	tests := messagehandler.Body{
		Email:         "sunilsn@iitk.ac.in",
		Phone:         "7432094921",
		MessageBody:   "hellohiytttty",
		Transactionid: "125456456",
		Customerid:    "347656r76789",
		Key:           "14244",
	}

	m, _ := json.Marshal(tests)

	/*** tests if the msg is send sucesfully or not
	 **/

	for i := 0; i < 3; i++ {

		sendflag := messagehandler.Send(m, i)
		assert.Equal(t, true, sendflag)

	}
	/***  test  for handle insert
	when mail server is down ,this finc will insert message to database
	**/

	for j := 0; j < 3; j++ {
		insertflag := messagehandler.HandleInsert(m, j)
		assert.Equal(t, true, insertflag)
	}
	for j := 0; j < 3; j++ {
		assert.Equal(t, true, messagehandler.HandleFailure(j))
	}
}
