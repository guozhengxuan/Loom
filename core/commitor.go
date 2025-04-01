package core

type Commitor struct {
	submitCh <-chan Slot
	reqCh chan<- Message
}

func (c *Commitor) commit(slot Slot) {
	blockCh := make(chan *Block)
	
}

func (c *Commitor) run() {
	for {
		select {
		case slot := <-c.submitCh:
			c.commit(slot)
		}
	}
}
