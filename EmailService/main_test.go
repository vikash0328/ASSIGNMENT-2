package main

import (
	"testing"

	"github.com/go-playground/assert/v2"
)

type PartitionOffset struct {
	Partition int
	Offset    int64
}

func TestOffsetManagment(t *testing.T) {

	if !InitVip() {
		t.Error("Unable to open Viper file")
	}
	logger = initZapLog()

	//TEST CASES
	/*********************************************************************************
	  when new partition comes in kafka broker so first it will insert it i.e. return 2


	  when partition exit and offset of provided partition is greater than the offset in
	  database then it should update its offset i.e. return 3


	  when the provided offset of given partition is less than offset of partition in database
	  then it should stop consumer from sending messages and poll for next messages i.e. 1


	  In case failure it return 0



	  **************************************************************************************/

	case1 := []PartitionOffset{{Partition: 6, Offset: 121},
		{Partition: 6, Offset: 122},
		{Partition: 6, Offset: 122}}

	case2 := []PartitionOffset{{Partition: 6, Offset: 121},
		{Partition: 7, Offset: 122},
		{Partition: 6, Offset: 122}}

	case3 := []PartitionOffset{{Partition: 6, Offset: 123},
		{Partition: 7, Offset: 121},
		{Partition: 7, Offset: 122}}

	// expected ouput
	expectedouput1 := []int{2, 3, 1}
	expectedouput2 := []int{1, 2, 1}
	expectedoutput3 := []int{3, 1, 1}

	for i := 0; i < 3; i++ {
		s := check(case1[i].Partition, case1[i].Offset)
		assert.Equal(t, expectedouput1[i], s)
	}
	for i := 0; i < 3; i++ {
		s := check(case2[i].Partition, case2[i].Offset)
		assert.Equal(t, expectedouput2[i], s)
	}
	for i := 0; i < 3; i++ {
		s := check(case3[i].Partition, case3[i].Offset)
		assert.Equal(t, expectedoutput3[i], s)
	}
	logger.Sync()
}
