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

var fsm [2]int64
var PrevOffset [2]int64  // helping for rebalancing
var PrevPartition [2]int //helping for rebancing
var u [2]bool
var DBEmailFail [2]bool
var OffsetFail [2]bool
var StopInsert [2]bool
var StateEmail [2]int

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
func OffsetMangment(j int, Partition int, Offset int64, wg *sync.WaitGroup) {
	if fsm[j] != 1 && PrevPartition[j] == Partition {
		UpdateOffset(j, Partition, Offset)

	}

	wg.Done()
}

//handle each and every case related to email comming from broker
func EmailMangment(j int, m kafka.Message, wg *sync.WaitGroup) {

	var body Body
	json.Unmarshal(m.Value, &body)
	logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
	logger.Info(string(m.Value))

	u[j] = Send(m.Value, j) //sending message for  email-server

	wg.Done()

}

func parallelHandleEmail(j int, wg *sync.WaitGroup) {
	var wg1 sync.WaitGroup
	r := kafkareader()
	defer r.Close()
	PrevPartition[j] = -1 // helping for rebalancing
	PrevOffset[j] = 0     // helping for rebalancing
	var t int
	u[j] = true
	fsm[j] = 1
	for {

		//intial case when consumer start or when their is message in database and our email server is up
		if u[j] && (StateEmail[j] == 1 || fsm[j] == 1) {
			StopInsert[j] = true
			HandleFailure(j)
			StopInsert[j] = false

		}
		//poll message from kafka-broker
		m, err := r.FetchMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}
		//checking the condition  rebalancing in consumer group
		if fsm[j] == 1 || PrevPartition[j] != m.Partition || PrevOffset[j] >= m.Offset {
			PrevPartition[j] = m.Partition
			PrevOffset[j] = m.Offset
			t = Check(m.Partition, m.Offset)
			OffsetCases(t, m.Partition)
			//when t=1 means we aready processed that message from given partition
		}
		if t != 1 {
			wg1.Add(2)
			go OffsetMangment(j, m.Partition, m.Offset, &wg1) //update the offset from given partition
			go EmailMangment(j, m, &wg1)                      //send the email to provided email-id
			wg1.Wait()

			if DBEmailFail[j] {
				OffsetMangment(j, m.Partition, m.Offset-1, &wg1)
				//indicating that email server as well as database server id down
				logger.Warn("Goroutine stopped", zap.Int("Goroutineid:", j))
				return
			}

			if OffsetFail[j] {
				r.CommitMessages(context.Background(), m)
				//as the offset mongodb server fail so we have to commit message

			} else if fsm[j]%8 == 0 {
				r.CommitMessages(context.Background(), m)
				//commiting to kafka-broker after recieving every 8th message
			}
			fsm[j] = fsm[j] + 1
			PrevOffset[j] = m.Offset
			PrevPartition[j] = m.Partition
		}

	}

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
	StateEmail[0] = 0
	StateEmail[1] = 0

	DBEmailFail[0] = false //indicating that till now their no failure in connecting db as well as email-server
	DBEmailFail[1] = false

	OffsetFail[0] = false //indicating that offset managment mongodb server is running
	OffsetFail[1] = false

	StopInsert[0] = false //indicating case when handfailure stop inserting same message in database(i.e. consumer up for 2nd time)
	StopInsert[1] = false
	wg.Add(2)
	for j := 0; j < 2; j++ {
		go parallelHandleEmail(j, &wg)
	}
	wg.Wait()
}
