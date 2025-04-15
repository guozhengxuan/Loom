package core

import (
	"Wahoo++/core"
	"Wahoo++/crypto"
	"Wahoo++/logger"
	"Wahoo++/pool"
	"testing"
)

func initTestConifg() {
	logger.SetLevel(logger.TestLevel)
}

func getBatch(batchSize int) pool.Batch {
	batch := pool.Batch{ID: 0}
	for i := 0; i < batchSize; i++ {
		batch.Txs = append(batch.Txs, make(pool.Transaction, 16))
	}
	return batch
}

func getBlock(batchSize int) *core.Block {
	block := &core.Block{
		Header: core.Header{
			Slot:      core.Slot{Author: -1, Height: -1},
			Round:     0,
			FirstRefH: 0,
		},
		Batch: getBatch(batchSize),
		Ref:   make([]core.Header, 0),
	}
	return block
}

func getDigest() crypto.Digest {
	return crypto.NewHasher().Sum256([]byte("123"))
}

func GetMsg(Typ int, sigService *crypto.SigService) core.NetMessage {
	var msg core.NetMessage
	switch Typ {
	case core.EchoType:
		msg, _ = core.NewEcho(core.NodeID(-1), getBlock(10), sigService)
	case core.ProposeType:
		msg = getBlock(10)
	case core.ReplyBlockType:
		msg, _ = core.NewReplyBlockMsg(-1, []*core.Block{getBlock(10)}, -1, sigService)
	case core.RequestBlockType:
		msg, _ = core.NewRequestBlock(-1, []crypto.Digest{getDigest()}, -1, 0, sigService)
	case core.ElectType:
		msg, _ = core.NewElectMsg(-1, -1, sigService)
	}
	return msg
}

func DisplayMsg(msg core.NetMessage, t *testing.T) {
	switch msg.MsgType() {

	case core.EchoType:
		temp := msg.(*core.Echo)
		t.Logf("%v \n", temp)
	case core.ProposeType:
		temp := msg.(*core.Block)
		t.Logf("%v \n", temp)
	case core.ElectType:
		temp := msg.(*core.Elect)
		t.Logf("%v \n", temp)
	case core.RequestBlockType:
		temp := msg.(*core.RequestBlockMsg)
		t.Logf("%v \n", temp)
	case core.ReplyBlockType:
		temp := msg.(*core.ReplyBlockMsg)
		t.Logf("%v \n", temp)
	}
}
