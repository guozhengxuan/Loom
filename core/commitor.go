package core

type Commitor struct {
	commitReqCh <-chan commitReq
	blockPullCh chan<- blockReq
}

func (c *Commitor) commit(req commitReq) {
	// Send block pull request.
}

func (c *Commitor) run() {
	for {
		select {
		case req := <-c.commitReqCh:
			c.commit(req)
		}
	}
}
