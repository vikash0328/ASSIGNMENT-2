package emailhandler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var fsm int64
var PrevOffset int64  // helping for rebalancing
var PrevPartition int //helping for rebancing
var u bool
var DBEmailFail bool
var OffsetFail bool
var StopInsert bool

//printing various scenario to logger during during partition assignment and rebalancing
func OffsetCases(s int, Partition int) {

	switch s {

	case 0:
		logger.Warn("You Might Get Duplicate Messages .Their is Problem in DataBase Connection")

	case 1:
		logger.Info("SuccesFully Stop From Duplicating Message")

	case 2:
		logger.Info("Your New Partion SuccesFully Entered", zap.Int("NewPartion:", Partition))

	default:

	}

}

//handle offset for particular partition after rebalancing in group
func OffsetMangment(Partition int, Offset int64, wg *sync.WaitGroup) {
	if fsm != 1 && PrevPartition == Partition {
		UpdateOffset(Partition, Offset)

	}
	wg.Done()
}

//handle each and every case related to email comming from broker
func EmailMangment(m kafka.Message, wg *sync.WaitGroup) {

	var body Body
	json.Unmarshal(m.Value, &body)
	logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
	logger.Info(string(m.Value))

	u = Send(m.Value)

	wg.Done()

}

//reading configuration for joining particular group and topic
func kafkareader() *kafka.Reader {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{viper.GetString("Brokers")},
		GroupID:        viper.GetString("GroupName"),
		Topic:          viper.GetString("Topic"),
		CommitInterval: 5 * time.Second,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
	})
	return r
}

//handle each and every case after recieving message from broker
func RecieveAndHandleEmail() {
	var wg sync.WaitGroup
	State_email = 0
	fsm = 1
	r := kafkareader()
	defer r.Close()
	PrevPartition = -1
	PrevOffset = 0
	DBEmailFail = false //indicating that till now their no failure in connecting db as well as email-server
	OffsetFail = false  //indicating that offset managment mongodb server is running
	StopInsert = false
	u = true
	for {

		//intial case when consumer start or when their is message in database and our email server is up
		if u && (State_email == 1 || fsm == 1) {
			StopInsert = true
			State_email = HandleFailure()
			StopInsert = false

		}
		//poll message from kafka-broker
		m, err := r.FetchMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}
		//checking the condition  rebalancing in consumer group
		if fsm == 1 || PrevPartition != m.Partition || PrevOffset >= m.Offset {
			PrevPartition = m.Partition
			PrevOffset = m.Offset
			t := Check(m.Partition, m.Offset)
			OffsetCases(t, m.Partition)
			//when t=1 means we aready processed that message from given partition
			if t == 1 {
				continue
			}
		}
		wg.Add(2)
		go OffsetMangment(m.Partition, m.Offset, &wg) //update the offset from given partition
		go EmailMangment(m, &wg)                      //send the email to provided email-id
		wg.Wait()
		if DBEmailFail {
			//indicating that email server as well as database server id down
			return
		}

		if OffsetFail {
			r.CommitMessages(context.Background(), m)
			//as the offset mongodb server fail so we have to commit message

		} else if fsm%8 == 0 {
			r.CommitMessages(context.Background(), m)
			//commiting to kafka-broker after recieving every 8th message
		}
		fsm = fsm + 1
		PrevOffset = m.Offset
		PrevPartition = m.Partition

	}

}
