package core

import (
	"WuKong/crypto"
	"sync"
)

type ElectAggregator struct {
	elects []*Elect
	used   map[NodeID]struct{}
}

func (a *ElectAggregator) Append(elect *Elect, committee Committee, sigService *crypto.SigService) (NodeID, error) {
	if _, ok := a.used[elect.Author]; ok {
		return NONE, ErrUsedElect(ElectType, elect.StrongRefRound, elect.Author)
	} else {
		a.used[elect.Author] = struct{}{}
		a.elects = append(a.elects, elect)
		if len(a.elects) == committee.HightThreshold() {
			var shares []crypto.SignatureShare
			for _, e := range a.elects {
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

type Elector struct {
	mu         *sync.RWMutex
	leaders    map[int]NodeID
	aggregator map[int]*ElectAggregator
	sigService *crypto.SigService
	committee  Committee
}

func NewElector(sigService *crypto.SigService, committee Committee) *Elector {
	return &Elector{
		mu:         &sync.RWMutex{},
		leaders:    make(map[int]NodeID),
		aggregator: make(map[int]*ElectAggregator),
		sigService: sigService,
		committee:  committee,
	}
}

func (e *Elector) Add(elect *Elect) (NodeID, error) {

	waveNum := elect.StrongRefRound / WaveRound
	a, ok := e.aggregator[waveNum]
	if !ok {
		a = &ElectAggregator{
			used:   make(map[NodeID]struct{}),
			elects: make([]*Elect, 0),
		}
		e.aggregator[waveNum] = a
	}
	id, err := a.Append(elect, e.committee, e.sigService)
	if err == nil && id != NONE {
		e.mu.Lock()
		e.leaders[waveNum] = id
		e.mu.Unlock()
	}
	return id, err
}

func (e *Elector) GetLeader(waveNum int) NodeID {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if leader, ok := e.leaders[waveNum]; ok {
		return leader
	} else {
		return NONE
	}
}
