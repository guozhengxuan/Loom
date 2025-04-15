package core_test

import (
	"Wahoo++/config"
	"Wahoo++/core"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewAggregator() *core.Aggregator {
	committee, _, _ := config.GenDefaultCommittee(4)
	return core.NewAggregator(&committee)
}

func TestAggregatorPush(t *testing.T) {
	ag := NewAggregator()

	author := core.NodeID(1)

	var msg1 core.NetMessage
	ag.Push(author, msg1)

	// Push another message from the same author.
	var msg2 core.NetMessage
	ag.Push(author, msg2)

	assert.Len(t, ag.Item, 1, "Duplicate author should not be added")
}

func TestAggregatorTake(t *testing.T) {
	thld := NewAggregator().Committee.HightThreshold()

	tests := []struct {
		msgCnt  int
		takeRep int
	}{
		{1, 1},
		{thld - 1, 1},
		{thld, 2},
		{thld + 1, 5},
	}

	for _, tt := range tests {
		ag := NewAggregator()

		for i := 0; i < tt.msgCnt; i++ {
			var msg core.NetMessage
			ag.Push(core.NodeID(i), msg)
		}

		for i := 0; i < tt.takeRep; i++ {
			res := ag.Take()

			assert.True(t, func(msgCnt, curTake int) bool {
				if curTake > 0 {
					return res == nil
				}
				return tt.msgCnt >= thld && res != nil ||
					tt.msgCnt < thld && res == nil

			}(tt.msgCnt, i), "Take insufficient msgs or take repeatedly")
		}

	}
}
