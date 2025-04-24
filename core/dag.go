package core

import (
	"Wahoo++/logger"
)

type dag struct {
	nodeID    NodeID
	committee *Committee

	cache     [][]*Block     // store blocks of the entire DAG
	cacheMark map[NodeID]int // height of highest committed block

	leader    map[int]NodeID // uncommitted leaders of each round.
	roundMark int            // highest committed round.

	opCh     chan Message
	submitCh chan submitReq

	pending       map[Slot]chan<- *Block // register one-shot reply channel for pending replies.
	pendingCommit *commitReq
}

func NewDag(nodeID NodeID, committee *Committee, opCh chan Message) *dag {
	dag := &dag{
		nodeID:    nodeID,
		committee: committee,
		cache:     make([][]*Block, committee.Size()),
		cacheMark: make(map[NodeID]int, committee.Size()),
		leader:    make(map[int]NodeID, 10),
		roundMark: -2,
		opCh:      opCh,
		submitCh:  make(chan submitReq),
		pending:   make(map[Slot]chan<- *Block),
	}

	for i := 0; i < committee.Size(); i++ {
		dag.cacheMark[NodeID(i)] = -1
	}

	return dag
}

func (d *dag) get(author NodeID, height int) *Block {
	i := height - d.cacheMark[author] - 1
	if i >= 0 && i < len(d.cache[author]) {
		return d.cache[author][i]
	}

	return nil
}

func (d *dag) handleCheckReq(req *checkReq) {
	var miss []Header

	for _, header := range req.items {
		b := header.Slot

		index := b.Height - d.cacheMark[b.Author] - 1

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

	index := b.Height - d.cacheMark[b.Author] - 1

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
	for r := d.roundMark + 1; r <= curR; r++ {
		if _, ok := d.leader[r]; !ok {
			return false
		}
	}
	return true
}

func (d *dag) handleCommitReq(req *commitReq) {
	logger.Debug.Printf("DAG handling commit request of round %d, leader ID: %d\n",
		req.round,
		req.leader)

	// Store leaders of each round.
	d.leader[req.round] = req.leader

	highR := req.round
	if d.pendingCommit != nil && d.pendingCommit.round > highR {
		highR = req.round
	}

	// Delay commit if lack of some previous leaders.
	if !d.isAnchorReady(highR) {
		if d.pendingCommit == nil || d.pendingCommit.round < req.round {
			d.pendingCommit = req
		}
		return
	}

	// It's safe to submit all previous blocks starting from the one
	// in the second position before the leader's highest block.
	line := d.cache[req.leader]
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == nil {
			continue
		}

		b := line[i].Header
		if b.Round == req.round && b.Slot.Height-b.FirstRefH >= 2 {
			d.submitCh <- submitReq{Slot{req.leader, b.Slot.Height - 2}, d.leader}
			return
		}
	}
}

func (d *dag) handleBlockPullReq(req *blockPullReq) {
	logger.Debug.Printf("DAG handling pull request of block from %d at height %d\n",
		req.slot.Author,
		req.slot.Height)

	b := req.slot
	replyCh := req.blockPullCh

	// Empty reply for committed blocks.
	if b.Height <= d.cacheMark[b.Author] {
		replyCh <- nil
		return
	}

	// Reply the block if received, otherwise wait for outer core to deliver.
	index := b.Height - d.cacheMark[b.Author] - 1
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
	for id := range req.newWatermark {
		next := req.newWatermark[id] - d.cacheMark[id]

		if len(d.cache[id]) > 0 {
			d.cache[id] = d.cache[id][next:]
		}

		d.cacheMark[id] = req.newWatermark[id]
	}

	// Remove committed anchors.
	d.roundMark = d.pendingCommit.round
	for r := range d.leader {
		if r < d.roundMark {
			delete(d.leader, r)
		}
	}

	// Clear pending entries.
	for s := range d.pending {
		delete(d.pending, s)
	}
	d.pendingCommit = nil
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
