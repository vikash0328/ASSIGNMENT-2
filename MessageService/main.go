package main

import (
	"fmt"

	"swap/MessageService/messagehandler"

	"go.uber.org/zap"
)

var logger *zap.Logger

func main() {

	if !messagehandler.InitVip() {
		fmt.Println("Unable to open Viper file")
		return
	}
	logger = messagehandler.InitZapLog()
	messagehandler.PassRefLog(&(*logger))
	defer logger.Sync()
	messagehandler.RecieveAndHandleMail()

}
