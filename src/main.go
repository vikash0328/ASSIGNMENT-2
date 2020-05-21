package main

import (
	"github.com/gin-gonic/gin"

	"swap/src/handler"

	"go.uber.org/zap"
)

var logger *zap.Logger

func main() {

	if !handler.InitViper() {
		return
	}

	logger = handler.InitZapLog()
	defer logger.Sync()

	//	w := Getkafkawriter()
	//defer w.Close()
	handler.PassRefLog(&(*logger))
	r := gin.Default()
	v := r.Group("/")
	{
		v.POST("/home", handler.Handlepost)
		v.POST("/home/:Partition", handler.HandlepostPartition)
	}
	r.Run()

}
