package core

import "WuKong/crypto"

type Aggregator interface {
	push()
	ready() bool 
}

type EchoAggregator struct {
	votes []*Message
	used map[NodeID]struct{}
	handled bool
	threshold int
}

func NewEchoAggregator(threshold int) *EchoAggregator {
	aggregator := &EchoAggregator{
		used: make(map[NodeID]struct{}),
		threshold: threshold,
	}
	return aggregator
}

func (ag *EchoAggregator) push(echo *Message) {
	if _, ok := ag.used[echo.Author]; ok {
		return
	}
	ag.used[echo.Author] = struct{}{}
	ag.votes = append(ag.votes, echo)
}

func (ag *EchoAggregator) ready() bool {
	if !ag.handled && len(ag.votes) == ag.threshold {
		ag.handled = true
		return true
	}
	return false
}

type ElectAggregator struct {
	shares []*Elect
	used map[NodeID]struct{}
	handled bool
	threshold int
}

func (ag *ElectAggregator) push(echo *Elect) {
	
}

func (ag *ElectAggregator) Append(elect *Elect, committee Committee, sigService *crypto.SigService) (NodeID, error) {
	if _, ok := ag.used[elect.Author]; ok {
		return NONE, ErrUsedElect(ElectType, elect.StrongRefRound, elect.Author)
	} else {
		ag.used[elect.Author] = struct{}{}
		ag.elects = append(ag.elects, elect)
		if len(ag.elects) == committee.HightThreshold() {
			var shares []crypto.SignatureShare
			for _, e := range ag.elects {
				shares = append(shares, e.SigShare)
			}
			qc, err := crypto.CombineIntactTSPartial(shares, sigService.ShareKey, elect.Hash())
			if err != nil {
				return NONE, err
			}
			var randint NodeID = 0
			for i := 0; i < 4; i++ {
				randint = randint<<8 + NodeID(qc[i])
			}
			return randint % NodeID(committee.Size()), nil
		}
	}
	return NONE, nil
}