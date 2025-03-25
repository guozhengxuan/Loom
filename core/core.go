package core

import (
	"WuKong/crypto"
	"WuKong/logger"
	"WuKong/pool"
	"WuKong/store"
	"sync"
	"time"
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
	loopBackChannel chan *Block
	commitChannel   chan<- *Block
	proposedNotify  map[int]*sync.Mutex
	echoAg          map[int]*EchoAggregator
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
		echoAg:          make(map[int]*EchoAggregator),
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

func (corer *Core) checkReference(block *Block) (bool, []crypto.Digest) {
	var temp []crypto.Digest
	for d := range block.Ref.Content {
		temp = append(temp, d)
	}
	ok, missDeigest := corer.localDAG.IsReceived(temp...)
	return ok, missDeigest
}

func (corer *Core) generatorBlock(height, refRound int) (*Block, error) {
	logger.Debug.Printf("procesing generatorBlock height %d round %d \n", height, refRound)

	ref := corer.localDAG.TakeRef(refRound)

	block, err := NewBlock(corer.nodeID, height, corer.txpool.GetBatch(), ref, corer.sigService)
	return block, err
}

func (corer *Core) handlePropose(block *Block) error {
	b := block.Abstract
	logger.Debug.Printf("procesing propose height %d node %d \n", b.Height, b.Author)

	// Verify signature.
	if !block.Verify(corer.committee) {
		return ErrSignature(block.MsgType(), b.Height, b.Author)
	}

	// Store Block.
	if err := storeBlock(corer.store, block); err != nil {
		return err
	}

	// Check reference.
	if ok, miss := corer.checkReference(block); !ok {
		//retrieve miss block
		corer.retriever.requestBlocks(miss, b.Author, block.Abstract.Digest)

		return ErrReference(block.MsgType(), b.Height, b.Author)
	}

	// Add to local DAG.
	corer.addToDAG(block)

	// Send echo.
	echo, err := NewEcho(corer.nodeID, block, corer.sigService)
	if err != nil {
		logger.Warn.Println(err)
	}
	corer.transmitor.Send(corer.nodeID, b.Author, echo)

	return nil
}

func (corer *Core) addToDAG(block *Block) error {
	return nil
}

func (corer *Core) handleEcho(echo *Echo) error {
	b := echo.BlockAbstract
	logger.Debug.Printf("procesing echo height %d node %d \n", b.Height, b.Author)

	// Verify signature
	if !echo.Verify(corer.committee) {
		return ErrSignature(echo.MsgType(), b.Height, b.Author)
	}

	// Aggregate.
	ag := corer.echoAg[b.Height]
	if ag == nil {
		ag = NewEchoAggregator(corer.committee.HightThreshold())
		corer.echoAg[b.Height] = ag
	}
	ag.push(echo)
	if ag.ready() {
		corer.localDAG.UpdateGrade()
	}

	return nil
}

func (corer *Core) invokeElect(round int) error {
	// Elect a leader if we are in a strong ref round.
	if round%2 == 0 {
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
	}
	return nil
}

func (corer *Core) handleElect(elect *Elect) error {
	logger.Debug.Printf("procesing elect round %d node %d \n", elect.StrongRefRound, elect.Author)

	if leader, err := corer.eletor.Add(elect); err != nil {
		return err
	} else if leader != NONE {
		grade := corer.localDAG.GetGrade(elect.StrongRefRound-1, int(leader))
		logger.Debug.Printf("Elector: round %d leader %d grade %d \n", elect.StrongRefRound, leader, grade)
		if grade == 1 {
			corer.commitor.NotifyToCommit(elect.StrongRefRound)
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
		if block.Height%WaveRound == 0 {
			corer.localDAG.UpdateGrade(block.Height, int(block.Author), GradeOne)
		}

		//maybe execute more one
		storeBlock(corer.store, block)

		corer.handleOutPut(block.Height, block.Author, block.Hash(), block.Ref.Succinct)
	}

	go corer.retriever.processReply(reply)

	return nil
}

func (corer *Core) handleLoopBack(block *Block) error {
	logger.Debug.Printf("procesing block loop back round %d node %d \n", block.Height, block.Author)

	//GRBC round
	if block.Height%WaveRound == 0 {
		instance := corer.getGRBCInstance(block.Author, block.Height)
		go instance.processPropose(block)
	} else {
		return corer.handleOutPut(block.Height, block.Author, block.Hash(), block.Ref)
	}

	return nil
}

func (corer *Core) handleCallBack(req *callBackReq) error {
	logger.Debug.Printf("procesing block call back round %d node %d \n", req.round, req.nodeID)

	//Update grade
	corer.localDAG.UpdateGrade(req.round, int(req.nodeID), req.grade)

	//try to advance round
	if req.tag == UpdateGrade {
		return corer.advanceRound(req.round + 1)
	} else if req.tag == NotifyOutPut {
		return corer.handleOutPut(req.round, req.nodeID, req.digest, req.reference)
	}

	return nil
}

func (corer *Core) start() error {
	block, err := corer.generatorBlock(0, 0)
	
	if propose, err := NewGRBCProposeMsg(corer.nodeID, 0, block, corer.sigService); err != nil {
		logger.Error.Println(err)
		panic(err)
	} else {
		corer.transmitor.Send(corer.nodeID, NONE, propose)
		corer.transmitor.RecvChannel() <- propose
	}
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
