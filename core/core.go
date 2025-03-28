package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/pool"
	"WuKong/store"
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
	commitor        *Commitor
	localDAG        *LocalDAG
	dag             *dag
	loopBackChannel chan *Block
	commitChannel   chan<- *Block
	proposedNotify  map[int]*sync.Mutex
	voteAg          map[int]*aggregator
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
	corer := &Core{
		nodeID:          nodeID,
		committee:       committee,
		parameters:      parameters,
		txpool:          txpool,
		transmitor:      transmitor,
		sigService:      sigService,
		store:           store,
		loopBackChannel: loopBackChannel,
		commitChannel:   commitChannel,
		proposedNotify:  make(map[int]*sync.Mutex),
		localDAG:        NewLocalDAG(),
		dag:             NewDag(nodeID, &committee),
		voteAg:          make(map[int]*aggregator),
	}

	corer.retriever = NewRetriever(nodeID, store, transmitor, sigService, parameters, loopBackChannel)
	corer.eletor = NewElector(sigService, committee)
	corer.commitor = NewCommitor(corer.eletor, corer.localDAG, store, commitChannel, committee.Size())

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

func (corer *Core) generatorBlock(height, refRound int) (*Block, error) {
	logger.Debug.Printf("procesing generatorBlock height %d round %d \n", height, refRound)

	ref := corer.dag.selectRef(refRound)

	block, err := NewBlock(corer.nodeID, height, corer.txpool.GetBatch(), ref, corer.sigService)
	return block, err
}

func (corer *Core) handlePropose(block *Block) error {
	b := block.Header

	logger.Debug.Printf("procesing propose height %d node %d \n", b.Height, b.Author)

	// Verify signature.
	if !block.Verify(corer.committee) {
		return ErrSignature(block.MsgType(), b.Height, b.Author)
	}

	// Store Block.
	if err := storeBlock(corer.store, block); err != nil {
		return err
	}

	// Send echo.
	echo, err := NewEcho(corer.nodeID, block, corer.sigService)
	if err != nil {
		logger.Warn.Println(err)
	}
	corer.transmitor.Send(corer.nodeID, b.Author, echo)

	// Add to local DAG.
	corer.dag.add(block)

	return nil
}

func (corer *Core) handleEcho(echo *Echo) error {
	b := echo.Header
	logger.Debug.Printf("procesing echo height %d node %d \n", b.Height, echo.Author)

	// Verify signature
	if !echo.Verify(corer.committee) {
		return ErrSignature(echo.MsgType(), b.Height, echo.Author)
	}

	// Aggregate.
	ag, ok := corer.voteAg[b.Height]
	if !ok {
		ag = NewAggregator(&corer.committee)
		corer.voteAg[b.Height] = ag
	}
	ag.push(echo.Author, echo)
	if votes := ag.take(); len(votes) != 0 {
		// corer.localDAG.UpdateGrade()
	}

	return nil
}

func (corer *Core) invokeElect(refRound int) error {
	// Invoke election if we are in a strong ref round.
	if refRound%2 == 1 {
		elect, err := NewElectMsg(
			corer.nodeID,
			refRound,
			corer.sigService,
		)
		if err != nil {
			return err
		}
		corer.transmitor.Send(corer.nodeID, NONE, elect)
		corer.transmitor.RecvChannel() <- elect
	}
	return nil
}

func (corer *Core) handleElect(elect *Elect) error {
	logger.Debug.Printf("procesing elect round %d node %d \n", elect.RefRound, elect.Author)

	if err := corer.eletor.add(elect); err != nil {
		return err
	}

	ok, leader := corer.eletor.getLeader(elect.RefRound)
	if ok {
		grade := corer.localDAG.GetGrade(elect.RefRound-1, int(leader))
		logger.Debug.Printf("Elector: round %d leader %d grade %d \n", elect.RefRound, leader, grade)
		if grade == 1 {
			corer.commitor.NotifyToCommit(elect.RefRound)
		}
	}

	return nil
}

func (corer *Core) handleRequestBlock(request *RequestBlockMsg) error {
	logger.Debug.Println("procesing block request")

	//Step 1: verify signature
	if !request.Verify(corer.committee) {
		return ErrSignature(request.MsgType(), -1, request.Author)
	}

	go corer.retriever.processRequest(request)

	return nil
}

func (corer *Core) handleReplyBlock(reply *ReplyBlockMsg) error {
	logger.Debug.Println("procesing block reply")

	//Step 1: verify signature
	if !reply.Verify(corer.committee) {
		return ErrSignature(reply.MsgType(), -1, reply.Author)
	}

	for _, block := range reply.Blocks {
		if block.Ref.Round%2 == 0 {
			corer.localDAG.UpdateGrade(block.Height, int(block.Author), GradeOne)
		}

		//maybe execute more one
		storeBlock(corer.store, block)

		// Add block to DAG.
	}

	go corer.retriever.processReply(reply)

	return nil
}

func (corer *Core) handleLoopBack(block *Block) error {
	logger.Debug.Printf("procesing block loop back round %d node %d \n", block.Height, block.Author)

	// Add block to DAG.

	return nil
}

func (corer *Core) start() error {
	block, err := corer.generatorBlock(0, 0)
	if err != nil {
		return err
	}

	corer.transmitor.Send(corer.nodeID, NONE, block)
	corer.transmitor.RecvChannel() <- block

	return nil
}

func (corer *Core) Run() {
	if corer.nodeID >= NodeID(corer.parameters.Faults) {
		// Propose the first block.
		corer.start()

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
