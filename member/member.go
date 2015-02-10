package member

import (
	"fmt"
	"log"
	"time"
)

type Member struct {
	address   string
	heartbeat uint32
	timeout   uint32
	exitChan  chan struct{}
}

func NewMember(address string, heartbeat, timeout uint32) *Member {
	m := &Member{
		address:   address,
		heartbeat: heartbeat,
		timeout:   timeout,
		exitChan:  make(chan struct{}),
	}
	return m
}

func (m *Member) GetAddress() string {
	return m.address
}

func (m *Member) SetAddress(addr string) {
	m.address = addr
}

func (m *Member) GetHeartbeat() uint32 {
	return m.heartbeat
}

func (m *Member) SetHeartbeat(heartbeat uint32) {
	m.heartbeat = heartbeat
}

func (m *Member) GetTimeout() uint32 {
	return m.timeout
}

func (m *Member) SetTimeout(timeout uint32) {
	m.timeout = timeout
}

func (m *Member) IsEqual(other *Member) bool {
	if m.address == other.address {
		return true
	}
	return false
}

func (m *Member) String() string {
	return fmt.Sprintf("Member: %s; Heartbeat: %d; Timemout: %d",
		m.address, m.heartbeat, m.timeout)
}

func (m *Member) Start(lost chan *Member, timeout uint32) {
	m.timeout = timeout

	for {
		select {
		case <-time.After(time.Millisecond * time.Duration(timeout)):
			log.Print(fmt.Sprintf("Member %s lost", m.address))
			lost <- m
		case <-m.exitChan:
			log.Print(fmt.Sprintf("Received member %s heartbeat", m.address))
			return
		}
	}
}

func (m *Member) Exit() {
	m.exitChan <- struct{}{}
}

func (m *Member) Restart(lost chan *Member, timeout uint32) {
	m.Exit()
	m.Start(lost, timeout)
}
