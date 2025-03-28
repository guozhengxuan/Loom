package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/store"
	"sync"
)

type commitReq struct {
	round  int
	leader NodeID
}

type dag struct {
	mu *sync.RWMutex

	nodeID    NodeID
	committee *Committee

	cache         [][]*Block // store blocks of the entire DAG
	lastRefHeight []int      // track the height of each node's last ref block
	watermark     []int      // height of highest committed block of each node

	blockChan   <-chan *Block
	refReqChan  <-chan int
	refRespChan chan<- Ref
	commitChan  <-chan commitReq
}

func NewDag(nodeID NodeID,
	committee *Committee,
	blockRecvChan <-chan *Block,
	RefReqChan <-chan int,
	RefRespChan chan<- Ref,
	commitChan <-chan commitReq) *dag {
	return &dag{
		mu:            new(sync.RWMutex),
		nodeID:        nodeID,
		committee:     committee,
		cache:         make([][]*Block, committee.Size()),
		lastRefHeight: make([]int, committee.Size()),
		watermark:     make([]int, committee.Size()),
		blockChan:     blockRecvChan,
		refReqChan:    RefReqChan,
		refRespChan:   RefRespChan,
		commitChan:    commitChan,
	}
}

func (d *dag) get(author NodeID, height int) *Block {
	i := height - d.watermark[author] - 1
	if i >= 0 && i < len(d.cache[author]) {
		return d.cache[author][i]
	}

	return nil
}

// Add the newly received block into local DAG.
func (d *dag) add(block *Block) {
	d.mu.Lock()
	defer d.mu.Unlock()

	b := block.Header

	// Add to cache, offset by watermark.
	oldLen := len(d.cache[b.Author])
	newLen := b.Height - d.watermark[b.Author]

	d.cache[b.Author] = append(d.cache[b.Author], make([]*Block, newLen-oldLen)...)

	d.cache[b.Author][newLen-1] = block

	// Update the height of author's last ref block.
	if block.Ref.Type != Plain {
		d.lastRefHeight[b.Author] = b.Height
	}
}

// Collect references for new block.
func (d *dag) selectRef(round int) Ref {
	d.mu.RLock()
	defer d.mu.RUnlock()

	refItems := make([]BlockHeader, 0, d.committee.HightThreshold())

	// Check if there are n-f new blocks.
	for id, line := range d.cache {
		lastHeight := line[len(line)-1].Header.Height

		lastRefHeight := d.lastRefHeight[id]

		if round%2 == 1 && lastHeight-lastRefHeight >= 2 ||
			round%2 == 0 && lastHeight-lastRefHeight >= 1 {
			refItems = append(refItems, BlockHeader{NodeID(id), lastHeight})
		}
	}

	refType := StrongRef

	if round%2 == 0 {
		refType = WeakRef
	}

	// Otherwise the ref is Plain and only points to the parent block.
	if len(refItems) < d.committee.HightThreshold() {
		refType = Plain

		size := len(d.cache[d.nodeID])
		lastHeight := d.cache[d.nodeID][size-1].Header.Height

		refItems = []BlockHeader{{NodeID(d.nodeID), lastHeight}}
	}

	return Ref{round, refType, refItems}
}

func (d *dag) commit(commitReq commitReq) {
	round := commitReq.round
	leader := commitReq.leader

	all := d.cache[leader]
	if len(all) == 0 {
		return
	}

	// The index is in range, becuase both commit invocation and watermark updating
	// happen exactly once in each strong ref round.
	index := d.lastRefHeight[leader] - d.watermark[leader] - 1
	last := all[index]
	if last.Ref.Round < round-1 {
		return
	}

	// According to wahoo++, it's safe to submit all previous blocks starting from
	// the one in the second position before the leader's highest block.
	d.submit(leader, all[len(all)-1].Header.Height-2)
}

// Submit block in a tree-traversing way.
func (d *dag) submit(node NodeID, height int) {

}

func (d *dag) run() {
	for {
		select {
		case block := <-d.blockChan:
			d.add(block)
		case round := <-d.refReqChan:
			d.refRespChan <- d.selectRef(round)
		case commitReq := <-d.commitChan:
			d.commit(commitReq)
		}
	}
}

type LocalDAG struct {
	muBlock      *sync.RWMutex
	blockDigests map[crypto.Digest]NodeID // store hash of block that has received
	muDAG        *sync.RWMutex
	localDAG     map[int]map[NodeID]crypto.Digest // local DAG
	edgesDAG     map[int]map[NodeID]map[crypto.Digest]NodeID
	muGrade      *sync.RWMutex
	gradeDAG     map[int]map[NodeID]int
}

func NewLocalDAG() *LocalDAG {
	return &LocalDAG{
		muBlock:      &sync.RWMutex{},
		muDAG:        &sync.RWMutex{},
		muGrade:      &sync.RWMutex{},
		blockDigests: make(map[crypto.Digest]NodeID),
		localDAG:     make(map[int]map[NodeID]crypto.Digest),
		gradeDAG:     make(map[int]map[NodeID]int),
		edgesDAG:     make(map[int]map[NodeID]map[crypto.Digest]NodeID),
	}
}

// Check for missing digests.
func (local *LocalDAG) IsReceived(digests ...crypto.Digest) (bool, []crypto.Digest) {
	local.muBlock.RLock()
	defer local.muBlock.RUnlock()

	var miss []crypto.Digest
	var flag bool = true
	for _, d := range digests {
		if _, ok := local.blockDigests[d]; !ok {
			miss = append(miss, d)
			flag = false
		}
	}

	return flag, miss
}

func (local *LocalDAG) ReceiveBlock(round int, node NodeID, digest crypto.Digest, references map[crypto.Digest]NodeID) {
	local.muBlock.Lock()
	local.blockDigests[digest] = node
	local.muBlock.Unlock()

	local.muDAG.Lock()
	vslot, ok := local.localDAG[round]
	eslot := local.edgesDAG[round]
	if !ok {
		vslot = make(map[NodeID]crypto.Digest)
		eslot = make(map[NodeID]map[crypto.Digest]NodeID)
		local.localDAG[round] = vslot
		local.edgesDAG[round] = eslot
	}
	vslot[node] = digest
	eslot[node] = references

	local.muDAG.Unlock()
}

func (local *LocalDAG) TakeRef(round int) Ref {
	return Ref{}
}

func (local *LocalDAG) GetRoundReceivedBlockNums(round int) (nums, grade2nums int) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	local.muGrade.RLock()
	defer local.muGrade.RUnlock()

	nums = len(local.localDAG[round])
	if round%2 == 0 {
		for _, g := range local.gradeDAG[round] {
			if g == GradeTwo {
				grade2nums++
			}
		}
	}

	return
}

func (local *LocalDAG) GetReceivedBlock(round int, node NodeID) (crypto.Digest, bool) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	if slot, ok := local.localDAG[round]; ok {
		d, ok := slot[node]
		return d, ok
	}
	return crypto.Digest{}, false
}

func (local *LocalDAG) GetReceivedBlockReference(round int, node NodeID) (map[crypto.Digest]NodeID, bool) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	if slot, ok := local.edgesDAG[round]; ok {
		reference, ok := slot[node]
		return reference, ok
	}
	return nil, false
}

func (local *LocalDAG) GetRoundReceivedBlock(round int) (digests map[crypto.Digest]NodeID) {
	local.muDAG.RLock()
	defer local.muDAG.RUnlock()
	digests = make(map[crypto.Digest]NodeID)
	for id, d := range local.localDAG[round] {
		digests[d] = id
	}

	return digests
}

func (local *LocalDAG) GetGrade(round, node int) (grade int) {
	if round%2 == 0 {
		local.muGrade.RLock()
		if slot, ok := local.gradeDAG[round]; !ok {
			return 0
		} else {
			grade = slot[NodeID(node)]
		}
		local.muGrade.RUnlock()
	}
	return
}

func (local *LocalDAG) UpdateGrade(round, node, grade int) {
	if round%2 == 0 {
		local.muGrade.Lock()

		slot, ok := local.gradeDAG[round]
		if !ok {
			slot = make(map[NodeID]int)
			local.gradeDAG[round] = slot
		}
		if grade > slot[NodeID(node)] {
			slot[NodeID(node)] = grade
		}

		local.muGrade.Unlock()
	}
}

type Commitor struct {
	elector       *Elector
	commitChannel chan<- *Block
	localDAG      *LocalDAG
	commitBlocks  map[crypto.Digest]struct{}
	curWave       int
	notify        chan int
	inner         chan crypto.Digest
	store         *store.Store
	N             int
}

func NewCommitor(electot *Elector, localDAG *LocalDAG, store *store.Store, commitChannel chan<- *Block, N int) *Commitor {
	c := &Commitor{
		elector:       electot,
		localDAG:      localDAG,
		commitChannel: commitChannel,
		commitBlocks:  make(map[crypto.Digest]struct{}),
		curWave:       -1,
		notify:        make(chan int, 100),
		store:         store,
		inner:         make(chan crypto.Digest),
		N:             N,
	}
	go c.run()
	return c
}

func (c *Commitor) run() {

	go func() {
		for digest := range c.inner {
			if block, err := getBlock(c.store, digest); err != nil {
				logger.Warn.Println(err)
			} else {
				if block.Batch.Txs != nil {
					//BenchMark Log
					logger.Info.Printf("commit Block round %d node %d batch_id %d \n", block.Header.Height, block.Header.Author, block.Batch.ID)
				}
				c.commitChannel <- block
			}
		}
	}()

	for num := range c.notify {
		if num > c.curWave {
			if ok, leader := c.elector.getLeader(num); ok {
				var leaderQ [][2]int
				for i := 1; i <= c.N; i++ {
					var node int = (int(leader) + i) % c.N
					if c.localDAG.GetGrade(2*num, node) == GradeTwo {
						leaderQ = append(leaderQ, [2]int{node, 2 * num})
					}
				}

				for i := num - 1; i > c.curWave; i-- {
					if ok, node := c.elector.getLeader(i); ok {
						leaderQ = append(leaderQ, [2]int{int(node), i * 2})
					}
				}
				c.commitLeaderQueue(leaderQ)
				c.curWave = num
			}

		}
	}
}

func (c *Commitor) commitLeaderQueue(q [][2]int) {

	for i := len(q) - 1; i >= 0; i-- {

		leader, round := q[i][0], q[i][1]
		var (
			queue1 []crypto.Digest
			queue2 []NodeID
			sortC  []crypto.Digest
		)
		if d, ok := c.localDAG.GetReceivedBlock(round, NodeID(leader)); !ok {
			logger.Error.Println("commitor : not received block")
			continue
		} else {
			queue1, queue2 = append(queue1, d), append(queue2, NodeID(leader))
			for len(queue1) > 0 {

				n := len(queue1)
				temp := make([]*crypto.Digest, c.N)

				for n > 0 {
					block, node := queue1[0], queue2[0]
					if _, ok := c.commitBlocks[block]; !ok {

						sortC = append(sortC, block)       // seq commit vector
						c.commitBlocks[block] = struct{}{} // commit flag

						if ref, ok := c.localDAG.GetReceivedBlockReference(round, node); !ok {
							logger.Error.Println("commitor : not received block reference")
						} else {

							for d, nodeid := range ref {
								temp[nodeid] = &d
							}

						}

					}
					queue1, queue2 = queue1[1:], queue2[1:]
					n--
				} //for

				//next round is pbc round
				if round%2 == 0 {
					for j := 0; j < c.N; j++ {
						if temp[j] != nil {
							queue1 = append(queue1, *temp[j])
							queue2 = append(queue2, NodeID(j))
						}
					}
				} else { //next round id grbc round
					// L := int(c.elector.GetLeader((round / 2)))
					// for j := 0; j < c.N; j++ {
					// 	ind := (L + c.N - j) % c.N
					// 	if temp[ind] != nil {
					// 		queue1 = append(queue1, *temp[ind])
					// 		queue2 = append(queue2, NodeID(ind))
					// 	}
					// }
				}
				round--
			} //for
		}

		for i := len(sortC) - 1; i >= 0; i-- {
			c.inner <- sortC[i] // SeqCommit
		}

	} //for
}

func (c *Commitor) NotifyToCommit(waveNum int) {
	c.notify <- waveNum
}
