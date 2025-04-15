package core_test

import (
	"Wahoo++/core"
	"testing"
	"time"
)

type mockCommittee struct {
	size           int
	hightThreshold int
}

func (m *mockCommittee) Size() int                      { return m.size }
func (m *mockCommittee) HightThreshold() int            { return m.hightThreshold }
func (m *mockCommittee) PublicKey(_ core.NodeID) []byte { return nil }
func (m *mockCommittee) VerifyProposal(_ []byte) error  { return nil }

func TestNewDag(t *testing.T) {
	committee := &mockCommittee{size: 3, hightThreshold: 2}
	d := core.NewDag(core.NodeID(0), committee, nil, nil)

	if d.nodeID != NodeID(0) {
		t.Errorf("Expected nodeID 0, got %d", d.nodeID)
	}

	if len(d.watermark) != 3 {
		t.Errorf("Expected watermark size 3, got %d", len(d.watermark))
	}

	for id, h := range d.watermark {
		if h != -1 {
			t.Errorf("Expected watermark -1 for node %d, got %d", id, h)
		}
	}
}

func TestDag_Get(t *testing.T) {
	committee := &mockCommittee{size: 2, hightThreshold: 1}
	d := NewDag(NodeID(0), committee, nil, nil)

	author := NodeID(1)
	height := 0
	d.watermark[author] = -1
	d.cache[author] = []*Block{{Header: Header{Slot: Slot{Author: author, Height: 0}}}}

	block := d.get(author, 0)
	if block == nil || block.Header.Slot.Height != 0 {
		t.Error("Failed to get existing block")
	}

	block = d.get(author, 1)
	if block != nil {
		t.Error("Unexpected block for height beyond cache")
	}
}

func TestDag_HandleCheckReq(t *testing.T) {
	committee := &mockCommittee{size: 2, hightThreshold: 1}
	d := NewDag(NodeID(0), committee, nil, nil)

	author := NodeID(1)
	d.watermark[author] = 0
	d.cache[author] = []*Block{nil, {Header: Header{Slot: Slot{Author: author, Height: 1}}}}

	req := &checkReq{
		items:      []Header{{Slot: Slot{Author: author, Height: 1}}, {Slot: Slot{Author: author, Height: 2}}},
		missRespCh: make(chan []Header, 1),
	}
	d.handleCheckReq(req)

	miss := <-req.missRespCh
	if len(miss) != 1 || miss[0].Slot.Height != 2 {
		t.Error("Incorrect missing headers")
	}
}

func TestDag_HandleBlockPushReq(t *testing.T) {
	committee := &mockCommittee{size: 2, hightThreshold: 1}
	d := NewDag(NodeID(0), committee, nil, nil)

	author := NodeID(1)
	block := &Block{Header: Header{Slot: Slot{Author: author, Height: 0}}}
	d.handleBlockPushReq(block)

	if len(d.cache[author]) != 1 || d.cache[author][0] != block {
		t.Error("Block not added to cache")
	}

	// Test expanding cache
	block2 := &Block{Header: Header{Slot: Slot{Author: author, Height: 2}}}
	d.handleBlockPushReq(block2)
	if len(d.cache[author]) != 3 || d.cache[author][2] != block2 {
		t.Error("Cache not expanded correctly")
	}
}

func TestDag_HandleRefReq(t *testing.T) {
	committee := &mockCommittee{size: 3, hightThreshold: 2}
	d := NewDag(NodeID(0), committee, nil, nil)

	// Setup caches for two authors with sufficient blocks
	for i := 0; i < 2; i++ {
		author := NodeID(i)
		d.cache[author] = []*Block{
			{Header: Header{Slot: Slot{Author: author, Height: 0}, FirstRefH: 0}},
			{Header: Header{Slot: Slot{Author: author, Height: 1}, FirstRefH: 0}},
		}
	}

	req := &refReq{round: 1, refRespCh: make(chan []Header, 1)} // odd round
	d.handleRefReq(req)

	refs := <-req.refRespCh
	if len(refs) != 2 { // Only two authors meet the condition (lastH - lastRefH >=2)
		t.Errorf("Expected 2 refs, got %d", len(refs))
	}
}

func TestDag_HandleCommitReq(t *testing.T) {
	submitCh := make(chan Slot, 1)
	committee := &mockCommittee{size: 3, hightThreshold: 2}
	d := NewDag(NodeID(0), committee, nil, submitCh)

	leader := NodeID(1)
	d.cache[leader] = []*Block{
		{Header: Header{Slot: Slot{Author: leader, Height: 0}}},
		{Header: Header{Slot: Slot{Author: leader, Height: 1}}},
		{Header: Header{Slot: Slot{Author: leader, Height: 2}}},
	}
	d.watermark[leader] = -1

	req := &commitReq{leader: leader, round: 1}
	d.handleCommitReq(req)

	if d.anchor[1] != leader {
		t.Error("Anchor not set")
	}

	select {
	case s := <-submitCh:
		if s.Height != 0 {
			t.Errorf("Expected height 0, got %d", s.Height)
		}
	default:
		t.Error("No slot submitted")
	}
}

func TestDag_HandleBlockPullReq(t *testing.T) {
	committee := &mockCommittee{size: 2, hightThreshold: 1}
	d := NewDag(NodeID(0), committee, nil, nil)

	author := NodeID(1)
	d.watermark[author] = 0
	d.cache[author] = []*Block{nil, {Header: Header{Slot: Slot{Author: author, Height: 1}}}}

	// Existing block
	req := &blockPullReq{
		slot:        Slot{Author: author, Height: 1},
		blockPullCh: make(chan *Block, 1),
	}
	d.handleBlockPullReq(req)

	select {
	case b := <-req.blockPullCh:
		if b.Header.Slot.Height != 1 {
			t.Error("Incorrect block returned")
		}
	default:
		t.Error("No response received")
	}

	// Pending request
	req2 := &blockPullReq{
		slot:        Slot{Author: author, Height: 2},
		blockPullCh: make(chan *Block, 1),
	}
	d.handleBlockPullReq(req2)

	if _, ok := d.pending[req2.slot]; !ok {
		t.Error("Request not added to pending")
	}

	// Push the pending block
	block := &Block{Header: Header{Slot: Slot{Author: author, Height: 2}}}
	d.handleBlockPushReq(block)

	select {
	case b := <-req2.blockPullCh:
		if b != block {
			t.Error("Pending block not resolved")
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for block")
	}
}

func TestDag_HandleCleanReq(t *testing.T) {
	committee := &mockCommittee{size: 2, hightThreshold: 1}
	d := NewDag(NodeID(0), committee, nil, nil)

	// Initial state
	d.watermark[0] = 2
	d.cache[0] = []*Block{
		{Header: Header{Slot: Slot{Author: 0, Height: 3}}},
		{Header: Header{Slot: Slot{Author: 0, Height: 4}}},
	}

	d.anchor[1] = 0
	d.anchor[2] = 1

	req := &cleanReq{
		newWatermark: map[NodeID]int{0: 4},
		round:        2,
	}
	d.handleCleanReq(req)

	if len(d.cache[0]) != 1 || d.cache[0][0].Header.Slot.Height != 4 {
		t.Error("Cache not cleaned correctly")
	}

	if _, ok := d.anchor[1]; ok {
		t.Error("Old anchor not removed")
	}
}
