package core

type Aggregator interface {
	push()
	ready() bool 
}

type EchoAggregator struct {
	votes []*Echo
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

func (ag *EchoAggregator) push(echo *Echo) {
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
