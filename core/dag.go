package core

// "WuKong/crypto"
// "WuKong/logger"
// "WuKong/store"

type dag struct {
	nodeID    NodeID
	committee *Committee

	cache     [][]*Block     // store blocks of the entire DAG
	watermark []int          // height of highest committed block
	anchor    map[int]NodeID // leader of each round

	rwChan <-chan Message
	submitChan chan<- submitReq

	pending map[commitReq]chan<- *Block // register one-shot reply channel for commit requests.
}

func NewDag(nodeID NodeID, committee *Committee) *dag {
	dag := &dag{
		nodeID:    nodeID,
		committee: committee,
		cache:     make([][]*Block, committee.Size()),
		watermark: make([]int, committee.Size()),
		anchor:    make(map[int]NodeID, 4),
		rwChan:    make(<-chan Message),
	}

	for i := 0; i < committee.Size(); i++ {
		dag.watermark[i] = -1
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

	for _, b := range req.items {
		index := b.H - d.watermark[b.Author] - 1
		if index >= len(d.cache[b.Author]) || index >= 0 && d.cache[b.Author][index] == nil {
			miss = append(miss, b)
		}
	}

	req.missRespCh <- miss
}

// Add the newly received block into local DAG.
func (d *dag) handleBlockReq(block *Block) {
	b := block.Header
	index := b.H - d.watermark[b.Author] - 1

	if index < 0 {
		return
	}

	oldLen := len(d.cache[b.Author])

	if index < oldLen {
		// Receiving a lagged block.
		d.cache[b.Author][index] = block
	} else {
		// Add the up-to-date block to cache, offset by watermark.
		newLen := index + 1
		d.cache[b.Author] = append(d.cache[b.Author], make([]*Block, newLen-oldLen)...)
		d.cache[b.Author][index] = block
	}

	// Response to registered block pull channel.
	req := commitReq{b.Author, b.H}
	if replyCh, ok := d.pending[req]; ok {
		replyCh <- block
	}
}

// Collect references for new block.
func (d *dag) handleRefReq(req *refReq) {
	ref := make([]Header, 0, d.committee.HightThreshold())

	// Check if there are n-f new qualified blocks.
	for _, line := range d.cache {

		// Ignore nodes from which no blocks of current round are received.
		if len(line) == 0 || line[len(line)-1].Header.R < req.round-1 {
			continue
		}

		lastBlockHeader := line[len(line)-1].Header

		lastH := lastBlockHeader.H
		lastRefH := lastBlockHeader.FirstRefH

		// Strong ref round requires two new blocks received from each node,
		// while weak ref round requires only one new block.
		if req.round%2 == 1 && lastH-lastRefH >= 2 ||
			req.round%2 == 0 && lastH-lastRefH >= 1 {
			ref = append(ref, lastBlockHeader)
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

func (d *dag) handleCommitReq(req commitReq) {
	leader := req.author
	round := req.round

	// Store leaders of each round.
	d.anchor[round] = leader

	line := d.cache[leader]

	// According to wahoo++, it's safe to submit all previous blocks starting from
	// the one in the second position before the leader's highest block.
	if len(line) >= 3 {
		height := d.watermark[leader] + len(line) - 2

		submitReq := submitReq{leader, height}

		d.submitChan <- submitReq
	}
}

// // Recursively submit block in a tree-traversing way.
// func (d *dag) submit(node NodeID, round, height int) {
// 	d.submitAnchor(round)
// 	// for h := d.cache
// }

func (d *dag) submitAnchor(round int) {

}

func (d *dag) handleBlockPullReq(req blockReq) {
	author := req.commitReq.author
	height := req.commitReq.round

	replyCh := req.blockPullCh

	// Empty reply for committed blocks.
	if height <= d.watermark[author] {
		replyCh <- nil
		return
	}

	index := height - d.watermark[author] - 1

	// Reply block if received, otherwise wait for outer core to receive.
	if index < len(d.cache[author]) && d.cache[author][index] != nil {
		replyCh <- d.cache[author][index]
	} else {
		d.pending[req.commitReq] = replyCh
	}
}

func (d *dag) run() {

	blockPullCh := make(chan blockReq)
	commitor := Commitor{d.commitReqCh, blockPullCh}

	// Init commitor
	go commitor.run()

	// Handle requests from core and commitor.
	for {
		select {
		case msg := <- d.rwChan:
			switch msg.MsgType() {
			case ProposeType:
				d.add(msg.(*Block))
			case RefReqType:

			}
		}
	}
}
