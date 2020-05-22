package main

import (
	"fmt"

	"swap/EmailService/emailhandler"

	"go.uber.org/zap"
)

var logger *zap.Logger

func main() {

	if !emailhandler.InitVip() {
		fmt.Println("Unable to open Viper file")
		return
	}

	logger = emailhandler.InitZapLog()
	emailhandler.PassRefLog(&(*logger))
	defer logger.Sync()
	emailhandler.RecieveAndHandleEmail()

}
