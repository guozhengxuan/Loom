package core

import (
	"WuKong/crypto"
	"sync"
)

type aggregator struct {
	item      []Message
	used      map[NodeID]struct{}
	committee *Committee
}

func NewAggregator(committee *Committee) *aggregator {
	ag := &aggregator{
		used:      make(map[NodeID]struct{}),
		committee: committee,
	}
	return ag
}

func (ag *aggregator) push(author NodeID, msg Message) {
	if _, ok := ag.used[author]; ok {
		return
	}
	ag.used[author] = struct{}{}
	ag.item = append(ag.item, msg)
}

func (ag *aggregator) take() []Message {
	if len(ag.item) == ag.committee.HightThreshold() {
		return ag.item
	}
	return nil
}

type Elector struct {
	mu         *sync.RWMutex
	leader     map[int]NodeID
	ag         map[int]*aggregator
	sigService *crypto.SigService
	committee  Committee
}

func NewElector(sigService *crypto.SigService, committee Committee) *Elector {
	return &Elector{
		mu:         &sync.RWMutex{},
		leader:     make(map[int]NodeID),
		ag:         make(map[int]*aggregator),
		sigService: sigService,
		committee:  committee,
	}
}

func (e *Elector) add(elect *Elect) error {
	round := elect.RefRound

	e.mu.Lock()
	defer e.mu.RUnlock()

	a, ok := e.ag[round]
	if !ok {
		a = NewAggregator(&e.committee)
		e.ag[round] = a
	}

	msg := a.take()
	if len(msg) == 0 {
		return nil
	}

	shares := make([]crypto.SignatureShare, len(msg))
	sig, err := crypto.CombineIntactTSPartial(shares, e.sigService.ShareKey, elect.Hash())
	if err != nil {
		return err
	}

	e.leader[round] = e.reveal(sig)

	return nil
}

func (e *Elector) reveal(sig []byte) NodeID {
	var seed NodeID = 0
	for i := 0; i < 4; i++ {
		seed = seed<<8 + NodeID(sig[i])
	}
	return seed % NodeID(e.committee.Size())
}

func (e *Elector) getLeader(refRound int) (bool, NodeID) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if leader, ok := e.leader[refRound]; ok {
		return true, leader
	}
	return false, NONE
}
