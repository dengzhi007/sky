package common

import (
	"fmt"
	"time"
)

const (
	STATUS_NORMAL    int = 1
	STATUS_ABNORMAL  int = 0
	VALUE_TYPE_STR   int = 1
	VALUE_TYPE_FLOAT int = 0
	PRE_ITEM_NUM     int = 10
)

type Item struct {
	ItemId    int
	ItemName  string
	Value     string
	ValueType int
}

type Packet struct {
	Hostname   string
	Items      []*Item
	AgentTime  int64
	LocalTime  int64
	ItemNum    int
	kafkaBytes []byte
}

func NewPacket(host string, agent_time int64) *Packet {

	return &Packet{
		Hostname:  host,
		AgentTime: agent_time,
		LocalTime: time.Now().Unix(),
		Items: make([]*Item, 0, PRE_ITEM_NUM),
		ItemNum: 0,
	}
}

func (this *Packet) AddItem(item_id int, val string, item_name string) {
	if this.Items == nil {
		this.Items = make([]*Item, 0, PRE_ITEM_NUM)
		this.ItemNum = 0
	}	
	i := &Item{
		ItemId: item_id,
		ItemName: item_name,
		Value:  val,
		ValueType: VALUE_TYPE_STR,
	}
	this.Items = append(this.Items, i)
	this.ItemNum += 1
}

func (this *Packet) getKafkaBytes() {
	// qhids;hostname;localtime;itemid1:val1,itemid2:val2,
	s := fmt.Sprintf("qhids;%s;%d;", this.Hostname, this.LocalTime)	
	for _, item := range this.Items {
		s += fmt.Sprintf("%s:%s,", item.ItemName, item.Value)	
	}
	this.kafkaBytes = ([]byte)(s)
}

func (this *Packet) Encode() ([]byte, error) {
	if this.ItemNum < 1 {
		return nil, fmt.Errorf("item num < 1")
	}

	if this.kafkaBytes == nil {
		this.getKafkaBytes()
	}

	return this.kafkaBytes, nil
}

func (this *Packet) Length() int {
	if this.ItemNum < 1 {
		return 0
	}

	if this.kafkaBytes == nil {
		this.getKafkaBytes()
	}

	return len(this.kafkaBytes)
}
