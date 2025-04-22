package core

import (
	"Wahoo++/crypto"
)

type Aggregator struct {
	Item      []NetMessage
	Used      map[NodeID]struct{}
	Committee *Committee
}

func NewAggregator(committee *Committee) *Aggregator {
	ag := &Aggregator{
		Used:      make(map[NodeID]struct{}),
		Committee: committee,
	}
	return ag
}

func (ag *Aggregator) Push(author NodeID, msg NetMessage) {
	if _, ok := ag.Used[author]; ok {
		return
	}
	ag.Used[author] = struct{}{}
	ag.Item = append(ag.Item, msg)
}

func (ag *Aggregator) Take() []NetMessage {
	thld := ag.Committee.HightThreshold()

	if len(ag.Item) >= thld {
		// Only take once.
		res := ag.Item
		ag.Item = ag.Item[thld:]

		return res
	}
	
	return nil
}

type Elector struct {
	leader     map[int]NodeID
	ag         map[int]*Aggregator
	sigService *crypto.SigService
	committee  *Committee
}

func NewElector(sigService *crypto.SigService, committee *Committee) *Elector {
	return &Elector{
		leader:     make(map[int]NodeID),
		ag:         make(map[int]*Aggregator),
		sigService: sigService,
		committee:  committee,
	}
}

func (e *Elector) Add(elect *Elect) error {
	round := elect.Round

	a, ok := e.ag[round]
	if !ok {
		a = NewAggregator(e.committee)
		e.ag[round] = a
	}
	a.Push(elect.Author, elect)

	msg := a.Take()

	// Exit if the number of elect msgs did't met threshold.
	if len(msg) == 0 {
		return nil
	}

	shares := make([]crypto.SignatureShare, len(msg))
	for i, e := range msg {
		shares[i] = e.(*Elect).SigShare
	}

	// Aggregate partial sigs to a complete one.
	sig, err := crypto.CombineIntactTSPartial(shares, e.sigService.ShareKey, elect.Hash())
	if err != nil {
		return err
	}
	e.leader[round] = e.Reveal(sig)

	return nil
}

func (e *Elector) Reveal(sig []byte) NodeID {
	var seed NodeID = 0
	for i := 0; i < 4; i++ {
		seed = seed<<8 + NodeID(sig[i])
	}
	return seed % NodeID(e.committee.Size())
}

func (e *Elector) TryGetLeader(round int) (bool, NodeID) {
	if leader, ok := e.leader[round]; ok {
		return true, leader
	}
	return false, NONE
}

func (e *Elector) RemoveBy(round int) {
	delete(e.leader, round)
	delete(e.ag, round)
}
