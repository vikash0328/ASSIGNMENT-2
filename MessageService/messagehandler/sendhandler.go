package messagehandler

import (
	"encoding/json"
	"net/smtp"

	"go.uber.org/zap"
)

// send message to provided email-id
func Send(m []byte, j int) bool {
	var body Body
	//binding for converting byte data struct type Body
	json.Unmarshal(m, &body)

	from := "..."
	pass := "..."
	to := body.Phone + "@sms.clicksend.com" // receivers mobile number +N number  format
	msg := "Your Transaction is Completed:" + "\n" + "Transaction_id: " + string(body.Transactionid) + "\n" + "Customer_id: " + string(body.Customerid) + "\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		logger.Error(err.Error())
		//condition when database already have message i.e. when HandleFailure() called
		if StopInsert[j] == true {
			logger.Warn("Sucessfully Stop From duplicating Message in Database")
			return false
		}
		//case when email server id down so insert data into database
		if HandleInsert(m, j) {
			StateEmail[j] = 1 //it indicate that their is message in  the collection  for jth goroutine database to send
			return false
		}
		DBEmailFail[j] = 1 // indicating failure in connecting database as well as email server
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}
