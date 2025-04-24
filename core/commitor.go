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

func (c *Commitor) commitAnchor(anchor Slot, pulled map[Slot]*Block) {
	var ordered []*Block

	q := []Slot{anchor}
	for len(q) > 0 {
		head := q[0]
		q = q[1:]

		cur, ok := pulled[head]
		if !ok {
			continue
		}

		ordered = append(ordered, cur)

		for _, ref := range cur.Ref {
			q = append(q, ref.Slot)
		}
	}

	sort.Slice(ordered, func(i, j int) bool {
		s1, s2 := ordered[i].Header.Slot, ordered[j].Header.Slot
		if s1.Author == s2.Author {
			return s1.Height < s2.Height
		}

		// For safety, blocks of current leader are in the tail position.
		if s1.Author == anchor.Author {
			return false
		}
		if s2.Author == anchor.Author {
			return true
		}

		return s1.Author < s2.Author
	})

	// Exec block and remove it from `pulled`.
	for _, b := range ordered {
		c.exec(b)
		delete(pulled, b.Header.Slot)
	}
}

func (c *Commitor) commit2(startSlot Slot, leaders map[int]NodeID) {
	logger.Debug.Printf("committing request for block of height %d node %d\n",
		startSlot.Height,
		startSlot.Author)

	// `anchors` stores the highest safe-to-commit block for each uncommitted leader.
	anchors := make(map[int]Slot, len(leaders))

	pulled := make(map[Slot]*Block)

	// Step 1: Pull all uncommitted blocks from dag.
	q := []Slot{startSlot}
	for len(q) > 0 {
		head := q[0]
		q = q[1:]

		// Skip pulled.
		if _, ok := pulled[head]; ok {
			continue
		}

		// Skip committed.
		cur := c.pullBlock(head)
		if cur == nil {
			continue
		}

		pulled[head] = cur

		// Update anchor.
		b := cur.Header
		if leader, ok := leaders[b.Round]; ok {
			if b.Slot.Author == leader && b.Slot.Height-b.FirstRefH >= 1 {
				if old, ok := anchors[b.Round]; !ok || old.Height < b.Slot.Height {
					anchors[b.Round] = cur.Header.Slot
				}
			}
		}

		for _, ref := range cur.Ref {
			q = append(q, ref.Slot)
		}
	}

	if len(pulled) == 0 {
		return
	}

	// Write new watermark.
	wm := make(map[NodeID]int)
	for b := range pulled {
		if h, ok := wm[b.Author]; !ok || b.Height > h {
			wm[b.Author] = b.Height
		}
	}

	// Step 2: Commit anchors in sequence to guarantee total order.
	rounds := make([]int, 0, len(anchors))
	for r := range anchors {
		rounds = append(rounds, r)
	}
	sort.Ints(rounds)

	for _, r := range rounds {
		c.commitAnchor(anchors[r], pulled)
	}

	// Step 3: Send cleanup request back to dag.
	req := gcReq{wm}
	c.reqCh <- &req
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
		c.commit2(req.slot, req.leader)
	}
}
