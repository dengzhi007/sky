package task

import (
	"common"
	"time"
	log "lib/log4go"
	"fmt"
)

type Filter struct {
	index         int
	running       bool
	heartbeat     int64
	logger        log.Logger
	inChan        chan *common.Packet
	outChan       chan []byte
}

func NewFilter(idx int, inputChan chan *common.Packet, outputChan chan []byte, logging log.Logger) *Filter {
	f := &Filter{
		index: idx,
		heartbeat: time.Now().Unix(),
		running: false,
		logger: logging,
		inChan: inputChan,
		outChan: outputChan,
	}

	if f == nil {
                fmt.Println("new filter failed")
                return nil
	}

	if f.logger == nil {
                fmt.Println("filter new logger failed")
                return nil
	}

	if f.inChan == nil {
                fmt.Println("filter must has an input channel")
                return nil
	}

	if f.outChan == nil {
                fmt.Println("filter must has an output channel")
                return nil
	}

	return f
}

func (this *Filter) Start() {
	// alarm packet from channel
	var ap *common.Packet
	var ok bool
	var key string
	var rules map[int]*common.Rule
	var rule_id int
	var rule *common.Rule
	var status int
	var bytes []byte
	var err error	

	this.running = true
	this.logger.Info("Filter%d start to work", this.index)

	for this.running {
		this.heartbeat = time.Now().Unix()
		ap, ok = <- this.inChan
		if !ok {
			this.logger.Error("Filter%d read from packet channel failed", this.index)
			time.Sleep(time.Second)
			continue
		}
		this.logger.Debug("Filter%d read from packet channel: hostname:%s, item id:%d, value:%d", this.index, ap.Hostname, ap.ItemId, ap.Value)	
		key = fmt.Sprintf("%s_%d", ap.Hostname, ap.ItemId)
		rules, ok = common.Rules[key]
		if !ok {
			this.logger.Debug("Filter%d read packet hostname:%s, item id:%d has no rule, drop", this.index, ap.Hostname, ap.ItemId)	
			continue
		}
		for rule_id, rule = range rules {
			status = rule.CheckStatus(ap.Value)
			bytes, err = ap.ConvertToBytesForBackend(rule_id, status)
			if err != nil {
				this.logger.Error("Filter%d read packet hostname:%s, item id:%d convert to rule bytes failed, error:%s", this.index, ap.Hostname, ap.ItemId, err.Error())	
				continue
			}
			err = this.output(bytes, this.outChan)
			if err != nil {
				this.logger.Error("Filter%d put rule bytes:%s into filter channel failed: %s", this.index, string(bytes), err.Error())	
				continue
			}
			this.logger.Info("Filter%d put rule bytes:%s into filter channel", this.index, string(bytes))	
		}
	}
}


func (this *Filter) output(bytes []byte, outChan chan []byte) error {
	select {
		case outChan <- bytes:
			return nil
		default:
			this.logger.Error("Filter%d insert into filter channel timeout, channel length:%d too more, drop packet bytes: %s", this.index, len(outChan), string(bytes))
			common.Alarm("filter_insert_channel_block")
			return fmt.Errorf("channel timeout")
	}
	return nil
}


func (this *Filter) Stop() {
	this.running = false
	this.logger.Info("Filter%d stop", this.index)
}
