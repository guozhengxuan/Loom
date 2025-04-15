package network_test

import (
	"Wahoo++/core"
	"Wahoo++/network"
	"sync"
	"testing"
	"time"
)

func TestNetwork(t *testing.T) {
	// logger.SetOutput(logger.InfoLevel|logger.DebugLevel|logger.ErrorLevel|logger.WarnLevel, logger.NewFileWriter("./default.log"))
	cc := network.NewCodec(core.DefaultNetMsgTypes)
	addr := ":8080"
	receiver := network.NewReceiver(addr, cc)
	go receiver.Run()
	time.Sleep(time.Second)
	sender := network.NewSender(cc)
	go sender.Run()

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(ind int) {
			defer wg.Done()
			msg := &network.NetMessage{
				Msg: &core.Echo{
					Author: 1,
					Header: core.Header{Slot: core.Slot{Author: 1, Height: 1}, Round: 0, FirstRefH: 0},
				},
				Address: []string{addr},
			}
			sender.Send(msg)
		}(i)
	}

	for i := 0; i < 10; i++ {
		msg := receiver.Recv().(*core.Echo)
		t.Logf("Messsage type: %d Data: %#v\n", msg.MsgType(), msg)
	}
	wg.Wait()
}
