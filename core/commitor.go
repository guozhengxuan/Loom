package core

import (
	"WuKong/logger"
	"sort"
)

type Commitor struct {
	submitCh <-chan Slot
	reqCh    chan<- Message
}

func (c *Commitor) pullBlock(slot Slot) *Block {
	blockCh := make(chan *Block)

	c.reqCh <- &blockPullReq{slot, blockCh}
	return <-blockCh
}

func (c *Commitor) pullLeader(round int) NodeID {
	leaderCh := make(chan NodeID)

	c.reqCh <- &leaderReq{round, leaderCh}
	return <-leaderCh
}

func (c *Commitor) commit(slot Slot) {
	cur := c.pullBlock(slot)

	// This is a committed block.
	if cur == nil {
		return
	}

	var q, ordered []*Block
	q = append(q, cur)

	for len(q) > 0 {
		head := q[0]

		ordered = append(ordered, head)
		q = q[len(q)-1:]

		// Add a plain block.
		if len(head.Ref) == 1 {
			if b := c.pullBlock(head.Ref[0].Slot); b != nil {
				q = append(q, b)
			}

			continue
		}

		// Request last round leader.
		lastR := cur.Header.Round - 1
		lastLeader := c.pullLeader(lastR)

		// Find the safe commit point of last round.
		var maxH int
		for _, ref := range cur.Ref {

			firstSlot := Slot{ref.Slot.Author, ref.FirstRefH}
			first := c.pullBlock(firstSlot)

			for _, ref2 := range first.Ref {

				if ref2.Slot.Author == lastLeader &&
					ref2.Round == lastR-1 &&
					ref2.Slot.Height > maxH {

					maxH = ref2.Slot.Height

					break
				}
			}
		}

		// Recursively commit highest block of last round's leader that could
		// have been committed by other nodes, so as to guarantee total order.
		c.commit(Slot{lastLeader, maxH - 1})

		sortedRef := cur.Ref
		sort.Slice(sortedRef, func(i, j int) bool {
			return sortedRef[i].Slot.Author < sortedRef[j].Slot.Author
		})

		// Add blocks belong to current commit.
		for _, prev := range sortedRef {
			if b := c.pullBlock(prev.Slot); b != nil {
				q = append(q, b)
			}
		}

	}

	wm := make(map[NodeID]int)

	// Reverse ordered blocks to execute.
	for i := len(ordered) - 1; i >= 0; i-- {

		// Update watermark.
		b := ordered[i].Header.Slot
		if _, ok := wm[b.Author]; !ok {
			wm[b.Author] = b.Height
		}

		c.exec(ordered[i])
	}

	// Help dag clean up committed blocks.
	req := cleanReq{cur.Header.Round, wm}
	c.reqCh <- &req
}

func (c *Commitor) exec(block *Block) {
	if block.Batch.Txs != nil {
		b := block.Header.Slot

		// BenchMark Log.
		logger.Info.Printf("commit Block height %d node %d batch_id %d \n", b.Height, b.Author, block.Batch.ID)
	}

	// TODO: implement execution of txs.
}

func (c *Commitor) run() {
	for slot := range c.submitCh {
		c.commit(slot)
	}
}
