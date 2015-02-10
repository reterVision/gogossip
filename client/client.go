package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/reterVision/gogossip/member"
)

type Configuration struct {
	MyAddress string
	Servers   []string
	Gossip    uint32
	Cleanup   uint32
}

type Heartbeat struct {
	client *Client
}

func NewHeartbeat(client *Client) *Heartbeat {
	h := Heartbeat{
		client: client,
	}

	return &h
}

func (h *Heartbeat) Receive(addr *string, reply *string) error {
	// Receive a heartbeat packet from one of the servers.
	if addr == nil {
		return errors.New("Nil member arrived")
	}

	var newMember *member.Member
	newMember, ok := h.client.memberList[*addr]
	if !ok {
		log.Print(fmt.Sprintf("%s not in member list", *addr))
		newMember, ok = h.client.deadList[*addr]
		if !ok {
			log.Print(fmt.Sprintf("%s not in dead list", *addr))
			errorMsg := fmt.Sprintf("Unknown member %s arrived", *addr)
			return errors.New(errorMsg)
		} else {
			// Resurrect the dead member
			log.Print(fmt.Sprintf("Resurrect %s from death", *addr))
			h.client.memberList[*addr] = newMember
			delete(h.client.deadList, *addr)
		}
	}

	// Reset the timeout for newly arrived member.
	h.client.memberChan <- newMember

	return nil
}

type Client struct {
	memberList map[string]*member.Member
	deadList   map[string]*member.Member
	gossip     uint32 // Time to gossip to members, in milliseconds.
	cleanup    uint32 // Time to mark a member as dead, in milliseconds.

	memberChan chan *member.Member
	memberLost chan *member.Member
	goodbye    chan bool

	configuration Configuration
}

func NewClient(config string) *Client {
	c := &Client{
		memberList: make(map[string]*member.Member),
		deadList:   make(map[string]*member.Member),
		memberChan: make(chan *member.Member),
		memberLost: make(chan *member.Member),
		goodbye:    make(chan bool),
	}

	// Load servers from config file
	file, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}

	decoder := json.NewDecoder(file)
	c.configuration = Configuration{}
	err = decoder.Decode(&c.configuration)
	if err != nil {
		log.Fatal(err)
	}

	// Setup gossip & cleanup timeout
	c.gossip = c.configuration.Gossip
	c.cleanup = c.configuration.Cleanup

	for _, server := range c.configuration.Servers {
		m := member.NewMember(server, c.gossip, c.cleanup)
		c.memberList[m.GetAddress()] = m
	}

	return c
}

func (c *Client) sendMemberList() {
	// Select one random member (except myself), send a heartbeat to it.
	serverList := []string{}
	for server, _ := range c.memberList {
		serverList = append(serverList, server)
	}

	var memberAddress string
	if len(serverList) > 0 {
		tries := 10
		for {
			rand.Seed(time.Now().Unix())
			memberAddress = serverList[rand.Intn(len(serverList))]
			if memberAddress != c.configuration.MyAddress {
				break
			}

			// Keep choosing random member until not choose myself.
			tries--
			if tries <= 0 {
				memberAddress = ""
				break
			}
		}
	}

	if memberAddress == "" {
		log.Print("Failed to choose member to send heartbeat")
		return
	}

	// Initialie RPC client for sending heartbeat calls.
	client, err := rpc.DialHTTP("tcp", memberAddress)
	if err != nil {
		log.Print(err)
		return
	}

	// Asynchronously send heartbeat.
	client.Go("Heartbeat.Receive", c.configuration.MyAddress, nil, nil)
}

func (c *Client) resetTimeout(m *member.Member) {
	m.Exit()
	go m.Start(c.memberLost, c.cleanup)
}

func (c *Client) Start(lostChan chan string) {
	// Start RPC service, waiting for heartbeat.
	go func() {
		index := strings.Index(c.configuration.MyAddress, ":")
		myPort := c.configuration.MyAddress[index:]

		h := NewHeartbeat(c)
		err := rpc.Register(h)
		if err != nil {
			log.Fatal(err)
		}
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", myPort)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
		http.Serve(l, nil)
	}()
	log.Print(fmt.Sprintf("RPC service started: %s", c.configuration.MyAddress))

	for _, m := range c.memberList {
		go m.Start(c.memberLost, c.cleanup)
	}

	for {
		select {
		case m := <-c.memberLost:
			// Add lost member to deadList and remove it from memberList
			memberAddress := m.GetAddress()
			c.deadList[memberAddress] = m
			delete(c.memberList, memberAddress)

			// Trigger customized behavior
			if lostChan != nil {
				lostChan <- memberAddress
			}
		case <-time.After(time.Millisecond * time.Duration(c.gossip)):
			c.sendMemberList()
		case m := <-c.memberChan:
			c.resetTimeout(m)
		}
	}
}
