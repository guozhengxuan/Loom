package core_test

import (
	"Wahoo++/config"
	"Wahoo++/core"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var once sync.Once

func getCommittee() *core.Committee {
	var committee core.Committee
	once.Do(func() {
		committee, _, _ = config.GenDefaultCommittee(4)
	})
	return &committee
}

func TestAggregator_Push_DuplicateAuthor(t *testing.T) {
	committee := getCommittee()
	ag := core.NewAggregator(committee)

	author := core.NodeID(1)

	var msg1 core.NetMessage
	ag.Push(author, msg1)

	// Push another message from the same author
	var msg2 core.NetMessage
	ag.Push(author, msg2)

	assert.Len(t, ag.Item, 1, "Duplicate author should not be added")
}

func TestAggregator_Take_ThresholdNotMet(t *testing.T) {
	committee := getCommittee()
	ag := core.NewAggregator(committee)

	for i := 0; i < committee.HightThreshold()-1; i++ {
		var msg core.NetMessage
		ag.Push(core.NodeID(i), msg)
	}

	result := ag.Take()
	assert.Nil(t, result, "Should return nil when threshold is not met")
}

func TestAggregator_Take_ThresholdMet(t *testing.T) {
	committee := getCommittee()
	ag := core.NewAggregator(committee)

	for i := 0; i < committee.HightThreshold(); i++ {
		var msg core.NetMessage
		ag.Push(core.NodeID(i), msg)
	}

	result := ag.Take()
	assert.NotNil(t, result, "Should return items when threshold is met")
}
