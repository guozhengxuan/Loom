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
	visited := make(map[Slot]bool)
	var ordered []Slot

	q := []Slot{anchor}
	for len(q) > 0 {
		head := q[0]
		q = q[1:]

		cur, ok := pulled[head]
		if !ok || visited[head] {
			continue
		}

		visited[head] = true
		ordered = append(ordered, head)

		for _, ref := range cur.Ref {
			q = append(q, ref.Slot)
		}
	}

	sort.Slice(ordered, func(i, j int) bool {
		s1, s2 := ordered[i], ordered[j]
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
	for _, s := range ordered {
		c.exec(pulled[s])
		delete(pulled, s)
	}
}

func (c *Commitor) commit2(req submitReq) {
	logger.Debug.Printf("committing request for block of height %d node %d\n",
		req.slot.Height,
		req.slot.Author)

	// `anchors` stores the highest safe-to-commit block for each uncommitted leader.
	anchors := make(map[int]Slot, len(req.uncommitted))
	anchors[req.round] = req.slot

	pulled := make(map[Slot]*Block)

	// Step 1: Pull all uncommitted blocks from dag.
	q := []Slot{req.slot}
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
		if leader, ok := req.uncommitted[b.Round]; ok {
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
		c.commit2(req)
	}
}
