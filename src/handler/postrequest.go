package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

var logger *zap.Logger //define globally so that it can used for logging in any method

/***************************************************************************************
* binding for post request response
*
*
*****************************************************************************************/

type Body struct {
	Email         string `json:"Email"`
	Phone         string `json:"Phone"`
	MessageBody   string `json:"MessageBody"`
	Transactionid string `json:"Transactionid"`
	Customerid    string `json:"Customerid"`
	Key           string `json:"Key"`
	Partition     int    `json:"partition"`
}

//setting reference to logger object that we intiliase in main

func PassRefLog(log *zap.Logger) {
	logger = log
}

//handle post request response for  specific return type

func handleResponse(s int, c *gin.Context) {

	switch s {
	case 0:
		c.JSON(200, gin.H{"message": "Error", "Body": "Couldn't complete your request"})

	case 1:
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message sent with Key"})
	case 2:
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message sent without Key"})
	default:
		c.JSON(200, gin.H{"message": "Success", "Body": "your Transaction is Completed", "Info": "Message has been sent to provided Partition"})

	}

}

func Handlepost(c *gin.Context) {
	var jbody Body
	//binding struct of type of Body

	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	fmt.Println(jbody)

	b, _ := json.Marshal(jbody)
	fmt.Println(jbody)
	var s int

	w := Getkafkawriter()
	defer w.Close()

	//checking for key  to send to kafka broker
	if jbody.Key != "" {
		s = Writemessagewithkey(w, []byte(jbody.Key), []byte(string(b)), &(*logger))
		//send with key

	} else {
		s = Writemessagewithkey(w, nil, []byte(string(b)), &(*logger))
		//send without key

	}
	//response for post request
	handleResponse(s, c)
}

func HandlepostPartition(c *gin.Context) {
	//geeting partition no. from param object in url
	partition, _ := strconv.Atoi(c.Param("Partition"))
	fmt.Println(partition)

	var jbody Body
	//binding struct of type of Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	fmt.Println(jbody)

	b, _ := json.Marshal(jbody)
	fmt.Println(jbody)
	var s int
	//checking for key  to send to kafka broker
	if jbody.Key != "" {
		s = WriteMessagewithPartition(partition, []byte(jbody.Key), []byte(string(b)), &(*logger))
		//send with key and partition

	} else {
		s = WriteMessagewithPartition(partition, nil, []byte(string(b)), &(*logger))
		//send without key,partition
	}
	//response for post request
	handleResponse(s, c)

}
