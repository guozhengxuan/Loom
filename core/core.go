package core

import (
	"Wahoo++/crypto"
	"Wahoo++/logger"
	"Wahoo++/pool"
	"Wahoo++/store"
	"sync"
)

type Core struct {
	nodeID          NodeID
	committee       Committee
	parameters      Parameters
	txpool          *pool.Pool
	transmitor      *Transmitor
	sigService      *crypto.SigService
	store           *store.Store
	retriever       *Retriever
	eletor          *Elector
	dagCh           chan Message
	loopBackChannel chan *Block
	commitChannel   chan<- *Block
	proposedNotify  map[int]*sync.Mutex
	voteAg          map[int]*Aggregator
}

func NewCore(
	nodeID NodeID,
	committee Committee,
	parameters Parameters,
	txpool *pool.Pool,
	transmitor *Transmitor,
	store *store.Store,
	sigService *crypto.SigService,
	commitChannel chan<- *Block,
) *Core {

	loopBackChannel := make(chan *Block, 1_000)
	dagCh := make(chan Message, 10_000)
	submitCh := make(chan Slot)

	// Init and run dag.
	dag := NewDag(nodeID, &committee, dagCh, submitCh)
	go dag.run()

	corer := &Core{
		nodeID:          nodeID,
		committee:       committee,
		parameters:      parameters,
		txpool:          txpool,
		transmitor:      transmitor,
		sigService:      sigService,
		store:           store,
		dagCh:           dagCh,
		loopBackChannel: loopBackChannel,
		commitChannel:   commitChannel,
		proposedNotify:  make(map[int]*sync.Mutex),
		voteAg:          make(map[int]*Aggregator),
	}

	corer.retriever = NewRetriever(nodeID, store, transmitor, sigService, parameters, loopBackChannel)
	corer.eletor = NewElector(sigService, &committee)

	return corer
}

func storeBlock(store *store.Store, block *Block) error {
	key := block.Hash()
	if val, err := block.Encode(); err != nil {
		return err
	} else {
		store.Write(key[:], val)
		return nil
	}
}

func getBlock(store *store.Store, digest crypto.Digest) (*Block, error) {
	block := &Block{}
	data, err := store.Read(digest[:])
	if err != nil {
		return nil, err
	}
	if err := block.Decode(data); err != nil {
		return nil, err
	}
	return block, nil
}

func (corer *Core) propose(height, round, oldFirstRefH int) error {
	b, err := corer.generateBlock(height, round, oldFirstRefH)

	if err != nil {
		return err
	}

	corer.transmitor.Send(corer.nodeID, NONE, b)
	corer.transmitor.RecvChannel() <- b

	return nil
}

func (corer *Core) generateBlock(height, round, oldFirstRefH int) (*Block, error) {
	logger.Debug.Printf("procesing generateBlock height %d round %d \n", height, round)

	// Request refs from dag.
	respCh := make(chan []Header)
	corer.dagCh <- &refReq{round, respCh}
	ref := <-respCh

	// If collected n-f refs, enter a new round.
	firstRefH := oldFirstRefH
	if len(ref) > 1 {
		
		firstRefH = height
		round++

		// Invoke leader election in odd rounds.
		if round%2 == 1 {
			corer.invokeElect(round)
		}
	}

	block, err := NewBlock(corer.nodeID, height, round, firstRefH, corer.txpool.GetBatch(), ref, corer.sigService)
	return block, err
}

func (corer *Core) handlePropose(block *Block) error {
	b := block.Header.Slot

	logger.Debug.Printf("procesing propose height %d node %d \n", b.Height, b.Author)

	// Verify signature.
	if !block.Verify(corer.committee) {
		return ErrSignature(block.MsgType(), b.Height, b.Author)
	}

	// Send echo.
	echo, err := NewEcho(corer.nodeID, block, corer.sigService)
	if err != nil {
		logger.Warn.Println(err)
	}
	corer.transmitor.Send(corer.nodeID, b.Author, echo)

	// Add to local DAG.
	corer.dagCh <- block

	return nil
}

func (corer *Core) handleEcho(echo *Echo) error {
	b := echo.Header.Slot
	logger.Debug.Printf("procesing echo height %d node %d \n", b.Height, echo.Author)

	// Verify signature.
	if !echo.Verify(corer.committee) {
		return ErrSignature(echo.MsgType(), b.Height, echo.Author)
	}

	// Aggregate.
	ag, ok := corer.voteAg[b.Height]
	if !ok {
		ag = NewAggregator(&corer.committee)
		corer.voteAg[b.Height] = ag
	}
	ag.Push(echo.Author, echo)

	// Decoupling of broadcast and conesnsus is obtained by immediately
	// producing next block without waiting for n-f refs as Wahoo does.
	if votes := ag.Take(); votes != nil {
		corer.propose(b.Height+1, echo.Header.Round, echo.Header.FirstRefH)
	}

	return nil
}

func (corer *Core) invokeElect(round int) error {
	elect, err := NewElectMsg(
		corer.nodeID,
		round,
		corer.sigService,
	)

	if err != nil {
		return err
	}

	corer.transmitor.Send(corer.nodeID, NONE, elect)
	corer.transmitor.RecvChannel() <- elect

	return nil
}

func (corer *Core) handleElect(elect *Elect) error {
	logger.Debug.Printf("procesing elect round %d node %d \n", elect.Round, elect.Author)

	if err := corer.eletor.Add(elect); err != nil {
		return err
	}

	// Reveal leader and try to commit.
	ok, leader := corer.eletor.TryGetLeader(elect.Round)
	if ok {
		corer.dagCh <- &commitReq{leader, elect.Round}
		corer.eletor.RemoveBy(elect.Round)
	}

	return nil
}

func (corer *Core) handleRequestBlock(request *RequestBlockMsg) error {
	logger.Debug.Println("procesing block request")

	// Verify signature
	if !request.Verify(corer.committee) {
		return ErrSignature(request.MsgType(), -1, request.Author)
	}

	go corer.retriever.processRequest(request)

	return nil
}

func (corer *Core) handleReplyBlock(reply *ReplyBlockMsg) error {
	logger.Debug.Println("procesing block reply")

	// Verify signature
	if !reply.Verify(corer.committee) {
		return ErrSignature(reply.MsgType(), -1, reply.Author)
	}

	for _, block := range reply.Blocks {
		storeBlock(corer.store, block)

		// Add block to DAG.
		corer.handlePropose(block)
	}

	go corer.retriever.processReply(reply)

	return nil
}

func (corer *Core) handleLoopBack(block *Block) error {
	b := block.Header.Slot

	logger.Debug.Printf("procesing block loop back round %d node %d \n", b.Height, b.Author)

	// Add block to DAG.
	corer.handlePropose(block)

	return nil
}

func (corer *Core) Run() {
	if corer.nodeID >= NodeID(corer.parameters.Faults) {

		// Propose the first block.
		corer.propose(0, 0, 0)

		for {
			var err error
			select {
			case msg := <-corer.transmitor.RecvChannel():
				{
					switch msg.MsgType() {
					case ProposeType:
						err = corer.handlePropose(msg.(*Block))
					case EchoType:
						err = corer.handleEcho(msg.(*Echo))
					case ElectType:
						err = corer.handleElect(msg.(*Elect))
					case RequestBlockType:
						err = corer.handleRequestBlock(msg.(*RequestBlockMsg))
					case ReplyBlockType:
						err = corer.handleReplyBlock(msg.(*ReplyBlockMsg))
					}
				}

			case block := <-corer.loopBackChannel:
				{
					err = corer.handleLoopBack(block)
				}
			}

			if err != nil {
				logger.Warn.Println(err)
			}

		}
	}
}
