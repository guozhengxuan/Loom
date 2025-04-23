package core

import (
	"Wahoo++/logger"
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

func (c *Commitor) commit(startSlot Slot) {
	logger.Debug.Printf("committing request for block of height %d node %d\n",
		startSlot.Height,
		startSlot.Author)

	pulled := make(map[Slot]*Block)

	q := []Slot{startSlot}
	for len(q) > 0 {
		head := q[0]
		q = q[1:]

		// Skip pulled blocks.
		if _, ok := pulled[head]; ok {
			continue
		}

		// Pull block from dag.
		cur := c.pullBlock(head)
		if cur == nil {
			continue
		}
		pulled[head] = cur

		// Handle references.
		switch len(cur.Ref) {
		case 0:
		case 1:
			q = append(q, cur.Ref[0].Slot)

		// Handle block with n-f refs.
		default:
			{
				// Step 1: To guarantee total order, recursively commit highest block of
				// last round's leader that could have been committed by other nodes.
				if curR := cur.Header.Round; curR%2 == 0 {

					if lastLeader := c.pullLeader(curR - 2); lastLeader != NONE {

						// Find the safe commit point.
						var maxH int
						for _, ref := range cur.Ref {
							firstSlot := Slot{ref.Slot.Author, ref.FirstRefH}

							first := c.pullBlock(firstSlot)
							if first == nil {
								continue
							}

							for _, ref2 := range first.Ref {
								if ref2.Slot.Author == lastLeader &&
									ref2.Round == curR-2 &&
									ref2.Slot.Height-ref2.FirstRefH >= 2 &&
									ref2.Slot.Height > maxH {

									maxH = ref2.Slot.Height

									break
								}
							}
						}

						if maxH > 0 {
							c.commit(Slot{lastLeader, maxH - 1})
						}
					}
				}

				for _, ref := range cur.Ref {
					q = append(q, ref.Slot)
				}
			}
		}
	}

	// Ignore committed blocks.
	if len(pulled) == 0 {
		return
	}

	// Step 2: Order all blocks blong to current commit.
	ordered := make([]Slot, 0, len(pulled))
	for s := range pulled {
		ordered = append(ordered, s)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Author == ordered[j].Author {
			return ordered[i].Height < ordered[j].Height
		}

		// For safety, blocks of current leader are in the tail position.
		if ordered[i].Author == startSlot.Author {
			return false
		}
		if ordered[j].Author == startSlot.Author {
			return true
		}

		return ordered[i].Author < ordered[j].Author
	})

	// Execute blocks and update watermark.
	var roundMark int
	wm := make(map[NodeID]int)
	for _, s := range ordered {
		c.exec(pulled[s])

		// Update round mark.
		r := pulled[s].Header.Round
		if r > roundMark {
			roundMark = r
		}

		// Update watermark.
		b := pulled[s].Header.Slot
		if h, ok := wm[b.Author]; !ok || b.Height > h {
			wm[b.Author] = b.Height
		}
	}

	// Help dag clean up committed blocks.
	req := gcReq{roundMark, wm}
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
	for slot := range c.submitCh {
		c.commit(slot)
	}
}
