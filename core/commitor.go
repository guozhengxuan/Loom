package core

import (
	"Wahoo++/logger"
	"sort"
)

type Commitor struct {
	submitCh <-chan submitReq
	reqCh    chan<- Message
}

func (c *Commitor) pullBlock(slot Slot) *Block {
	blockCh := make(chan *Block)

	c.reqCh <- &blockPullReq{slot, blockCh}
	return <-blockCh
}

// Returns all uncommitted blocks reachable from the entrance block.
func (c *Commitor) undecidedHistory(ent Slot) map[Slot]*Block {
	hist := make(map[Slot]*Block)
	q := []Slot{ent}

	for len(q) > 0 {
		head := q[0]
		q = q[1:]

		// Skip if already in history.
		if _, ok := hist[head]; ok {
			continue
		}

		// Pull block from DAG. Skip if committed (returns nil).
		cur := c.pullBlock(head)
		if cur == nil {
			continue
		}

		hist[head] = cur

		// Enqueue all references to trigger block retrieval.
		for _, ref := range cur.Ref {
			q = append(q, ref.Slot)
		}
	}

	return hist
}

func (c *Commitor) submit(H map[Slot]*Block, uncommitted []NodeID) {
	preH := make(map[Slot]*Block)

	for preR := len(uncommitted)-1; preR >= 0; preR-- {
		// Find all blocks from the previous leader at previous wave (round r-2).
		var ancS *Slot
		for _, block := range H {
			b := block.Header
			if b.Round == preR && b.Slot.Author == uncommitted[preR] {
				if ancS == nil || ancS.Height < b.Slot.Height {
					ancS = &b.Slot
				}
			}
		}
		if ancS != nil {
			preH = c.undecidedHistory(*ancS)
			c.submit(preH, uncommitted[:preR+1])
			break
		}
	}

	left := make(map[Slot]*Block)
	for s, b := range H {
		if _, in := preH[s]; !in {
			left[s] = b
		}
	}

	c.submitHistory(left, uncommitted[len(uncommitted)-1])
}

func (c *Commitor) submitHistory(H map[Slot]*Block, leader NodeID) {
	ordered := make([]*Block, 0, len(H))
	for _, block := range H {
		ordered = append(ordered, block)
	}

	sort.Slice(ordered, func(i, j int) bool {
		s1, s2 := ordered[i].Header.Slot, ordered[j].Header.Slot
		if s1.Author == s2.Author {
			return s1.Height < s2.Height
		}

		// For safety, blocks of current leader are in the tail position.
		if s1.Author == leader {
			return false
		}
		if s2.Author == leader {
			return true
		}

		return s1.Author < s2.Author
	})

	// Execute blocks in order.
	for _, block := range ordered {
		c.exec(block)
	}
}

func (c *Commitor) commit(req submitReq) {
	logger.Debug.Printf("committing request for block of height %d node %d\n",
		req.slot.Height,
		req.slot.Author)

	H := c.undecidedHistory(req.slot)

	c.submit(H, req.undecided)

	// Write new watermark for garbage collection.
	wm := make(map[NodeID]int)
	for b := range H {
		if h, ok := wm[b.Author]; !ok || b.Height > h {
			wm[b.Author] = b.Height
		}
	}

	// Send cleanup request back to dag.
	gcReq := gcReq{wm}
	c.reqCh <- &gcReq
}

func (c *Commitor) exec(block *Block) {
	b := block.Header.Slot

	logger.Debug.Printf("commit Block height %d node %d\n",
		b.Height,
		b.Author)

	// BenchMark Log.
	if block.Batch.Txs != nil {
		logger.Info.Printf("commit Block height %d node %d batch_id %d \n",
			b.Height,
			b.Author,
			block.Batch.ID)
	}

	// TODO: implement execution of txs.
}

func (c *Commitor) run() {
	for req := range c.submitCh {
		// c.commit(req.slot, req.round)
		c.commit(req)
	}
}
