package core

type dag struct {
	nodeID    NodeID
	committee *Committee

	cache     [][]*Block     // store blocks of the entire DAG
	watermark map[NodeID]int // height of highest committed block
	anchor    map[int]NodeID // highest leader block of each round that is safe to commit

	opCh     <-chan Message
	submitCh chan Slot

	pending map[Slot]chan<- *Block // register one-shot reply channel for commit requests.
}

func NewDag(nodeID NodeID, committee *Committee, opCh <-chan Message, submitCh chan Slot) *dag {
	dag := &dag{
		nodeID:    nodeID,
		committee: committee,
		cache:     make([][]*Block, committee.Size()),
		watermark: make(map[NodeID]int, committee.Size()),
		anchor:    make(map[int]NodeID, 4),
		opCh:      opCh,
		submitCh:  submitCh,
	}

	for i := 0; i < committee.Size(); i++ {
		dag.watermark[NodeID(i)] = -1
	}

	return dag
}

func (d *dag) get(author NodeID, height int) *Block {
	i := height - d.watermark[author] - 1
	if i >= 0 && i < len(d.cache[author]) {
		return d.cache[author][i]
	}

	return nil
}

func (d *dag) handleCheckReq(req *checkReq) {
	var miss []Header

	for _, header := range req.items {
		b := header.Slot

		index := b.Height - d.watermark[b.Author] - 1

		if index >= len(d.cache[b.Author]) || index >= 0 && d.cache[b.Author][index] == nil {
			miss = append(miss, header)
		}
	}

	req.missRespCh <- miss
}

// Add the newly received block into local DAG.
func (d *dag) handleBlockPushReq(block *Block) {
	b := block.Header.Slot

	index := b.Height - d.watermark[b.Author] - 1

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
	ref := make([]Header, 0, d.committee.HightThreshold())

	// Check if there are n-f new qualified blocks.
	for _, line := range d.cache {

		// Ignore nodes from which no blocks of current round are received.
		if len(line) == 0 || line[len(line)-1].Header.Round < req.round-1 {
			continue
		}

		latestB := line[len(line)-1].Header

		latestH := latestB.Slot.Height
		latestRefH := latestB.FirstRefH

		// Strong ref round requires two new blocks received from each node,
		// while weak ref round requires only one.
		if req.round%2 == 1 && latestH-latestRefH >= 2 ||
			req.round%2 == 0 && latestH-latestRefH >= 1 {
			ref = append(ref, latestB)
		}
	}

	// Otherwise the ref is Plain and only points to the parent block.
	if len(ref) < d.committee.HightThreshold() {
		size := len(d.cache[d.nodeID])
		lastBlockHeader := d.cache[d.nodeID][size-1].Header

		ref = []Header{lastBlockHeader}
	}

	req.refRespCh <- ref
}

func (d *dag) handleCommitReq(req *commitReq) {
	leader := req.leader
	round := req.round

	// Store leaders of each round.
	d.anchor[round] = leader

	line := d.cache[leader]

	// It's safe to submit all previous blocks starting from the one 
	// in the second position before the leader's highest block.
	if len(line) >= 3 {
		height := d.watermark[leader] + len(line) - 2

		b := Slot{leader, height}
		d.submitCh <- b
	}
}

func (d *dag) handleBlockPullReq(req *blockPullReq) {
	b := req.slot
	replyCh := req.blockPullCh

	// Empty reply for committed blocks.
	if b.Height <= d.watermark[b.Author] {
		replyCh <- nil
		return
	}

	index := b.Height - d.watermark[b.Author] - 1

	// Reply the block if received, otherwise wait for outer core to deliver.
	if index < len(d.cache[b.Author]) && d.cache[b.Author][index] != nil {
		replyCh <- d.cache[b.Author][index]
	} else {
		d.pending[b] = replyCh
	}
}

func (d *dag) handleLeaderReq(req *leaderReq) {
	req.leaderPullCh <- d.anchor[req.round]
}

func (d *dag) handleCleanReq(req *cleanReq) {

	// Remove committed blocks and update watermark.
	for id := range d.watermark {

		cutIndex := req.newWatermark[id] - d.watermark[id] - 1

		d.cache[id] = append([]*Block{}, d.cache[id][cutIndex:]...)
		d.watermark[id] = req.newWatermark[id]
	}

	// Remove committed anchors.
	for r := range d.anchor {
		if r < req.round {
			delete(d.anchor, r)
		}
	}
}

func (d *dag) run() {
	commitReqCh := make(chan Message)
	commitor := Commitor{d.submitCh, commitReqCh}

	// Init commitor
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
		case LeaderReqType:
			d.handleLeaderReq(msg.(*leaderReq))
		case CleanReqType:
			d.handleCleanReq(msg.(*cleanReq))
		}
	}
}
