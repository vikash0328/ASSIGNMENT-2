package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Body struct {
	Email         string `json:"Email"`
	Phone         string `json:"Phone"`
	MessageBody   string `json:"MessageBody"`
	Transactionid string `json:"Transactionid"`
	Customerid    string `json:"Customerid"`
	Key           string `json:"Key"`
	Partition     int    `json:"partition"`
}

func PassRefLog(log *zap.Logger) {
	logger = log
}

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
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	//logger = initZapLog()
	fmt.Println(jbody)

	b, _ := json.Marshal(jbody)
	fmt.Println(jbody)
	var s int

	w := Getkafkawriter()
	defer w.Close()
	if jbody.Key != "" {
		s = Writemessagewithkey(w, []byte(jbody.Key), []byte(string(b)), &(*logger))

	} else {
		s = Writemessagewithkey(w, nil, []byte(string(b)), &(*logger))

	}

	handleResponse(s, c)
}

func HandlepostPartition(c *gin.Context) {

	partition, _ := strconv.Atoi(c.Param("Partition"))

	var jbody Body
	if err := c.ShouldBindJSON(&jbody); err != nil {
		logger.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	//logger = initZapLog()
	fmt.Println(jbody)

	b, _ := json.Marshal(jbody)
	fmt.Println(jbody)
	var s int

	if jbody.Key != "" {
		s = WriteMessagewithPartition(partition, []byte(jbody.Key), []byte(string(b)), &(*logger))

	} else {
		s = WriteMessagewithPartition(partition, nil, []byte(string(b)), &(*logger))

	}
	handleResponse(s, c)

}
