package core

import (
	"Wahoo++/logger"
)

type dag struct {
	nodeID    NodeID
	committee *Committee

	cache [][]*Block // store blocks of the entire DAG

	decidedH    map[NodeID]int // height of highest committed block
	decidedR    int            // leaders of all rounds before decidedR are safely out of trace.
	uncommitted []NodeID       // uncommitted leaders of each round.

	opCh          chan Message           // read & write operations received from corer and commitor.
	submitCh      chan submitReq         // forward commit request from corer to commitor.
	pending       map[Slot]chan<- *Block // register one-shot reply channel for pending pulls.
	pendingCommit *commitReq             // commit requests in processing.
}

func NewDag(nodeID NodeID, committee *Committee, opCh chan Message) *dag {
	dag := &dag{
		nodeID:      nodeID,
		committee:   committee,
		cache:       make([][]*Block, committee.Size()),
		decidedH:    make(map[NodeID]int, committee.Size()),
		uncommitted: make([]NodeID, 0),
		decidedR:    0,
		opCh:        opCh,
		submitCh:    make(chan submitReq, 100),
		pending:     make(map[Slot]chan<- *Block),
	}

	for i := 0; i < committee.Size(); i++ {
		dag.decidedH[NodeID(i)] = -1
	}

	return dag
}

func (d *dag) get(author NodeID, height int) *Block {
	i := height - d.decidedH[author] - 1
	if i >= 0 && i < len(d.cache[author]) {
		return d.cache[author][i]
	}

	return nil
}

func (d *dag) handleCheckReq(req *checkReq) {
	var miss []Header

	for _, header := range req.items {
		b := header.Slot

		index := b.Height - d.decidedH[b.Author] - 1

		if index >= len(d.cache[b.Author]) || index >= 0 && d.cache[b.Author][index] == nil {
			miss = append(miss, header)
		}
	}

	req.missRespCh <- miss
}

// Add the newly received block into local DAG.
func (d *dag) handleBlockPushReq(block *Block) {
	b := block.Header.Slot

	logger.Debug.Printf("DAG handling push request of block from %d at height %d\n",
		b.Author,
		b.Height)

	index := b.Height - d.decidedH[b.Author] - 1

	if index < 0 {
		return
	}

	oldLen := len(d.cache[b.Author])

	if index < oldLen {
		// Received a lagged block.
		d.cache[b.Author][index] = block
	} else {
		// Add the up-to-date block to cache, offset by watermark.
		newLen := index + 1
		d.cache[b.Author] = append(d.cache[b.Author], make([]*Block, newLen-oldLen)...)
		d.cache[b.Author][index] = block
	}

	// Response to corresponding block pull channel if registered.
	if replyCh, ok := d.pending[b]; ok {
		replyCh <- block
	}
}

// Collect references for new block.
func (d *dag) handleRefReq(req *refReq) {
	logger.Debug.Printf("DAG handling ref request of round %d\n", req.round)

	ref := make([]Header, 0, d.committee.HightThreshold())

	// Check if there are n-f new qualified blocks.
	for _, line := range d.cache {

		// Ignore nodes from which no blocks of current round are received.
		if len(line) == 0 || line[len(line)-1].Header.Round < req.round {
			continue
		}

		latestB := line[len(line)-1].Header

		latestH := latestB.Slot.Height
		latestRefH := latestB.FirstRefH

		// Strong ref round requires two new blocks received from each node,
		// while weak ref round requires only one.
		if line[len(line)-1].Header.Round > req.round ||
			req.round%2 == 0 && latestH-latestRefH >= 2 ||
			req.round%2 == 1 && latestH-latestRefH >= 1 {
			ref = append(ref, latestB)
		}
	}

	// Otherwise the ref is Plain and only points to the parent block.
	if len(ref) < d.committee.HightThreshold() {
		size := len(d.cache[d.nodeID])
		if size > 0 {
			lastBlockHeader := d.cache[d.nodeID][size-1].Header
			ref = []Header{lastBlockHeader}
		}
	}

	req.refRespCh <- ref
}

func (d *dag) isAnchorReady(curR int) bool {
	for r := d.decidedR; r <= curR; r += 2 {
		if d.uncommitted[r] == NONE {
			return false
		}
	}
	return true
}

func (d *dag) handleCommitReq(req *commitReq) {
	logger.Debug.Printf("DAG handling commit request of round %d, leader ID: %d\n",
		req.round,
		req.leader)

	// Update the pending commit.
	if d.pendingCommit == nil || d.pendingCommit.round < req.round {
		d.pendingCommit = req
	}

	// Add leader to trace.
	if req.round < d.pendingCommit.round {
		d.uncommitted[(req.round-d.decidedR)/2] = req.leader
	} else {
		for len(d.uncommitted) < req.round-d.decidedR/2 {
			d.uncommitted = append(d.uncommitted, NONE)
		}
		d.uncommitted = append(d.uncommitted, req.leader)
	}

	// Delay commit if lack of some previous leader.
	if !d.isAnchorReady(d.pendingCommit.round) {
		return
	}

	// It's safe to submit all previous blocks starting from the one
	// in the second position before the leader's highest block.
	line := d.cache[d.pendingCommit.leader]
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == nil {
			continue
		}

		b := line[i].Header
		if b.Round == d.pendingCommit.round && b.Slot.Height-b.FirstRefH >= 2 {
			s := Slot{d.pendingCommit.leader, b.Slot.Height - 2}
			d.submitCh <- submitReq{s, d.pendingCommit.round, d.decidedR, d.uncommitted}
			return
		}
	}

	d.pendingCommit = nil
}

func (d *dag) handleBlockPullReq(req *blockPullReq) {
	logger.Debug.Printf("DAG handling pull request of block from %d at height %d\n",
		req.slot.Author,
		req.slot.Height)

	b := req.slot
	replyCh := req.blockPullCh

	// Empty reply for committed blocks.
	if b.Height <= d.decidedH[b.Author] {
		replyCh <- nil
		return
	}

	// Reply the block if received, otherwise wait for outer core to deliver.
	index := b.Height - d.decidedH[b.Author] - 1
	if index < len(d.cache[b.Author]) && d.cache[b.Author][index] != nil {
		replyCh <- d.cache[b.Author][index]
	} else {
		d.pending[b] = replyCh
	}
}

func (d *dag) handleCleanReq(req *gcReq) {
	logger.Debug.Printf("DAG handling clean request of freshly committed round %d\n",
		d.pendingCommit.round)

	// Remove committed blocks and update watermark.
	for id := range req.newH {
		next := req.newH[id] - d.decidedH[id]

		if len(d.cache[id]) > 0 {
			d.cache[id] = d.cache[id][next:]
		}

		d.decidedH[id] = req.newH[id]
	}

	// Update committed rounds.
	d.decidedR = d.pendingCommit.round
	d.uncommitted = d.uncommitted[len(d.uncommitted)-1:]
	d.pendingCommit = nil

	// Clear pending entries.
	for s := range d.pending {
		delete(d.pending, s)
	}
}

func (d *dag) run() {
	// Init commitor
	commitor := Commitor{d.submitCh, d.opCh}
	go commitor.run()

	// Handle requests from core and commitor.
	for msg := range d.opCh {
		switch msg.MsgType() {
		case ProposeType:
			d.handleBlockPushReq(msg.(*Block))
		case RefReqType:
			d.handleRefReq(msg.(*refReq))
		case CommitReqType:
			d.handleCommitReq(msg.(*commitReq))
		case BlockReqType:
			d.handleBlockPullReq(msg.(*blockPullReq))
		case CleanReqType:
			d.handleCleanReq(msg.(*gcReq))
		}
	}
}
