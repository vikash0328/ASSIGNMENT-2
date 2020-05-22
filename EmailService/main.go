package main

import (
	"fmt"

	"swap/EmailService/emailhandler"

	"go.uber.org/zap"
)

var logger *zap.Logger

//var state_email int

/*type Body struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"_id ,omitempty"`
	Email         string             `bson:"Email,omitempty" json:"Email,omitempty"`
	Phone         string             `bson:"Phone,omitempty" json:"Phone,omitempty"`
	MessageBody   string             `bson:"MessageBody,omitempty" json:"MessageBody,omitempty"`
	Transactionid string             `bson:"Transactionid,omitempty" json:"Transactionid,omitempty"`
	Customerid    string             `bson:"Customerid,omitempty" json:"Customerid,omitempty"`
	Key           string             `bson:"Key" json:"Key"`
}

func kakfareader() *kafka.Reader {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{viper.GetString("Brokers")},
		GroupID:        viper.GetString("GroupName"),
		Topic:          viper.GetString("Topic"),
		CommitInterval: 5 * time.Second,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})
	return r
}*/

func main() {
	/*var wg sync.WaitGroup
	state_email = 0
	emailhandler.State_email = state_email
	var fsm int64 = 1*/
	if !emailhandler.InitVip() {
		fmt.Println("Unable to open Viper file")
		return
	}

	logger = emailhandler.InitZapLog()
	emailhandler.PassRefLog(&(*logger))
	defer logger.Sync()
	emailhandler.RecieveAndHandleEmail()

	/*r := kakfareader()
	PrevPartition := -1
	var PrevOffset int64
	u := true
	for {
		if u && (state_email == 1 || fsm == 1) {
			state_email = emailhandler.HandleFailure()

		}
		m, err := r.FetchMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}

		if fsm == 1 || PrevPartition != m.Partition || PrevOffset >= m.Offset {
			PrevPartition = m.Partition
			PrevOffset = m.Offset
			t := emailhandler.Check(m.Partition, m.Offset)
			if t == 1 {
				logger.Info("SuccesFully Stop From Duplicating Message")
				continue

			} else if t == 0 {
				logger.Warn("You Might Get Duplicate Messages .Their is Problem in DataBase Connection")
			} else if t == 2 {
				logger.Info("Your New Partion SuccesFully Entered", zap.Int("NewPartion:", m.Partition))
			}

		}
		wg.Add(2)
		func() {
			if fsm != 1 && PrevPartition == m.Partition {
				emailhandler.UpdateOffset(m.Partition, m.Offset)

			}
			wg.Done()
		}()
		func() {
			var body Body
			json.Unmarshal(m.Value, &body)
			logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
			logger.Info(string(m.Value))

			u = emailhandler.Send(m.Value)
			state_email = emailhandler.State_email
			wg.Done()
		}()
		wg.Wait()
		fsm = fsm + 1
		if fsm%8 == 0 {
			r.CommitMessages(context.Background(), m)
		}

	}

	r.Close()
	*/
}
