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
var PrevOffset int64
var PrevPartition int
var u bool

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

func OffsetMangment(Partition int, Offset int64, wg *sync.WaitGroup) {
	if fsm != 1 && PrevPartition == Partition {
		UpdateOffset(Partition, Offset)

	}
	wg.Done()
}

func EmailMangment(m kafka.Message, wg *sync.WaitGroup) {

	var body Body
	json.Unmarshal(m.Value, &body)
	logger.Info("metadata", zap.String("Topic", m.Topic), zap.String("Key", string(m.Key)), zap.Int64("Offset", m.Offset))
	logger.Info(string(m.Value))

	u = Send(m.Value)

	wg.Done()

}

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

func RecieveAndHandleEmail() {
	var wg sync.WaitGroup
	State_email = 0
	fsm = 1
	r := kafkareader()
	defer r.Close()
	PrevPartition = -1
	PrevOffset = 0
	u = true
	for {
		if u && (State_email == 1 || fsm == 1) {
			State_email = HandleFailure()

		}
		m, err := r.FetchMessage(context.Background())

		if err != nil {
			logger.Error(err.Error())
			break
		}

		if fsm == 1 || PrevPartition != m.Partition || PrevOffset >= m.Offset {
			PrevPartition = m.Partition
			PrevOffset = m.Offset
			t := Check(m.Partition, m.Offset)
			OffsetCases(t, m.Partition)
			if t == 1 {
				continue
			}
		}
		wg.Add(2)
		go OffsetMangment(m.Partition, m.Offset, &wg)
		go EmailMangment(m, &wg)
		wg.Wait()
		fsm = fsm + 1
		PrevOffset = m.Offset
		if fsm%8 == 0 {
			r.CommitMessages(context.Background(), m)
		}

	}

}
