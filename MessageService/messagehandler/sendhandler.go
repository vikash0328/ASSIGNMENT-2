package messagehandler

import (
	"encoding/json"
	"net/smtp"

	"go.uber.org/zap"
)

// send message to provided email-id
func send(m []byte, j int) bool {
	var body Body
	//binding for converting byte data struct type Body
	json.Unmarshal(m, &body)

	from := "swapnil.bro123@gmail.com"
	pass := "Let@123#rt"
	to := body.Phone + "@sms.clicksend.com" // receivers mobile number +N number  format
	msg := "Your Trnsaction is Completed:" + "\n" + "Transaction_id: " + string(body.Transactionid) + "\n" + "Customer_id: " + string(body.Customerid) + "\n"

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", from, pass, "smtp.gmail.com"),
		from, []string{to}, []byte(msg))

	if err != nil {
		logger.Error(err.Error())
		//case when email server id down so insert data into database
		handleInsert(m, j)
		StateEmail[j] = 1 //it indicate that their is message in  the collection  for jth goroutine database to send
		return false
	}
	logger.Info("Successfully Send", zap.String("Transaction_id", body.Transactionid))

	return true
}
