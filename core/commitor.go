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

// undecidedHistory computes UNDECIDEDHISTORY(B) from Algorithm 5.
// Returns all uncommitted blocks reachable from B.
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

// submit implements the recursive SUBMIT(r, H) procedure from Algorithm 5.
// Returns the set of blocks in H that should be committed at this round.
func (c *Commitor) submit(r int, H map[Slot]*Block, uncommitted map[int]NodeID) map[Slot]*Block {
	// Line 18-19: if r ≤ decidedR ∨ H = ⊥ then return
	leader, ok := uncommitted[r-2]
	if !ok || len(H) == 0 {
		return make(map[Slot]*Block)
	}

	// Line 20: S ← {B ∈ H | B.p = leaders[r-2] ∧ B.r = r-2}
	// Find all blocks from the leader of round r-2 at round r-2.
	S := make([]*Block, 0)
	for _, block := range H {
		b := block.Header
		if b.Round == r-2 && b.Slot.Author == leader && b.Slot.Height-b.FirstRefH >= 1 {
			S = append(S, block)
		}
	}

	// Line 21: B_anc ← argmax{B'.h}
	var anc *Block
	for _, block := range S {
		if anc == nil || block.Header.Slot.Height > anc.Header.Slot.Height {
			anc = block
		}
	}

	Hpre := H // Line 22: H_pre ← H

	// Line 23-25: if B_anc ≠ ⊥ then
	if anc != nil {
		// Line 24: H_pre ← UNDECIDEDHISTORY(B_anc)
		Hpre = c.undecidedHistory(anc.Header.Slot)

		// Line 25: SUBMIT(r-1, H_pre)
		c.submit(r-2, Hpre, uncommitted)
	}

	// Line 26: SUBMITHISTORY(r, H \ H_pre)
	// Compute H \ H_pre (set difference).
	left := make(map[Slot]*Block)
	for slot, block := range H {
		if _, inHpre := Hpre[slot]; !inHpre {
			left[slot] = block
		}
	}

	return c.submitHistory(r, left, uncommitted)
}

// submitHistory implements SUBMITHISTORY(r, H) from Algorithm 5 lines 27-32.
// Marks blocks as decided and outputs them in the correct order.
func (c *Commitor) submitHistory(r int, H map[Slot]*Block, uncommitted map[int]NodeID) map[Slot]*Block {
	// Line 29: isDecided[B] ← true (implicitly done by committing)

	// Line 30: S ← {B ∈ H | B.p = leaders[r]}
	leader, ok := uncommitted[r]
	if !ok {
		return H
	}

	S := make([]*Block, 0)
	nonS := make([]*Block, 0)

	for _, block := range H {
		if block.Header.Slot.Author == leader {
			S = append(S, block)
		} else {
			nonS = append(nonS, block)
		}
	}

	// Line 31: output B ∈ H \ S in some deterministic order
	// Sort non-leader blocks deterministically.
	sort.Slice(nonS, func(i, j int) bool {
		s1, s2 := nonS[i].Header.Slot, nonS[j].Header.Slot
		if s1.Author == s2.Author {
			return s1.Height < s2.Height
		}
		return s1.Author < s2.Author
	})

	// Execute non-leader blocks.
	for _, block := range nonS {
		c.exec(block)
	}

	// Line 32: output B ∈ S in the height-ascending order
	sort.Slice(S, func(i, j int) bool {
		return S[i].Header.Slot.Height < S[j].Header.Slot.Height
	})

	// Execute leader blocks in height order.
	for _, block := range S {
		c.exec(block)
	}

	return H
}

func (c *Commitor) commit(req submitReq) {
	logger.Debug.Printf("committing request for block of height %d node %d\n",
		req.slot.Height,
		req.slot.Author)

	// Build initial history H starting from the requested slot.
	H := c.undecidedHistory(req.slot)

	if len(H) == 0 {
		return
	}

	// Call recursive SUBMIT(r, H) which will handle the entire commit logic.
	c.submit(req.round, H, req.ucLeaders)

	// Write new watermark for garbage collection.
	wm := make(map[NodeID]int)
	for slot := range H {
		if h, ok := wm[slot.Author]; !ok || slot.Height > h {
			wm[slot.Author] = slot.Height
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
