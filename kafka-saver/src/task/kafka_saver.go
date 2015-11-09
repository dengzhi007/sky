package task

import (
	"common"
	"fmt"
	log "lib/log4go"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

type KafkaSaver struct {
	index         int
	running       bool
	heartbeat     int64
	brokers       []string
	topic         string
	logger        log.Logger
	inChan        chan *common.Packet
	producer      sarama.AsyncProducer
}

func NewKafkaSaver(idx int, inputChan chan *common.Packet, logging log.Logger) *KafkaSaver {
	s := &KafkaSaver{
		index:         idx,
		heartbeat:     time.Now().Unix(),
		running:       false,
		logger:        logging,
		inChan:        inputChan,
	}

	if s == nil {
		fmt.Println("new KafkaSaver failed")
		return nil
	}

	if s.logger == nil {
		fmt.Println("KafkaSaver new logger failed")
		return nil
	}

	if s.inChan == nil {
		fmt.Println("KafkaSaver must has an input channel")
		return nil
	}

	brokers, err := common.Conf.String("kafka", "brokers")
	if err != nil {
		fmt.Println("KafkaSaver no brokers")
		return nil
	}
	s.brokers = strings.Split(brokers, ";")
	s.logger.Debug("brokers:%v", s.brokers)

	topic, err := common.Conf.String("kafka", "topic")
	if err != nil {
		fmt.Println("KafkaSaver no topic")
		return nil
	}
	s.topic = topic
	s.logger.Debug("topic:%s", s.topic)

	return s
}

func (this *KafkaSaver) Start() {
	//this.testConsumer()
	var p *common.Packet
	var ok bool
	var err error

	this.running = true
	this.logger.Info("KafkaSaver%d start to work", this.index)

	for this.running {
		this.heartbeat = time.Now().Unix()

		p, ok = <-this.inChan
		if !ok {
			this.logger.Error("KafkaSaver%d read from packet channel failed", this.index)
			time.Sleep(time.Second)
			continue
		}
		this.logger.Debug("KafkaSaver%d read from packet channel: hostname:%s, item num:%d", this.index, p.Hostname, p.ItemNum)

		//insert into kafka
		start := time.Now()
		err = this.send(p)	
		if err != nil {
			this.logger.Error("KafkaSaver%d send to kafka failed, packet drop:%v", this.index, p)
			time.Sleep(time.Second)
			if this.producer != nil {
				this.producer.Close()
				this.producer = nil
			}
		}

		this.logger.Debug("KafkaSaver%d send to kafka success, duration:%v, hostname:%s, item num:%d, time:%d", this.index, time.Now().Sub(start), p.Hostname, p.ItemNum, p.LocalTime)
	}
}

func (this *KafkaSaver) connect() error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	//config.Producer.Compression = sarama.CompressionGZIP   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(this.brokers, config)
	if err != nil {
		this.logger.Error("connect to kafka failed")
		return err
	}

	// We will just log if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		this.logger.Debug("wait for producer errors...")
		for err := range producer.Errors() {
			this.logger.Error("Failed to write access log entry:", err)
		}
	}()	
	
	go func() {
		this.logger.Debug("wait for success producer message...")
		for {
			pm, ok := <-producer.Successes()
			if !ok {
				this.logger.Error("get producer success message failed")
				break
			}
			msg, _ := pm.Value.Encode()
			length := pm.Value.Length()
			this.logger.Info("KafkaSaver%d producer send out message success, topic:%s, partition:%d, offset:%d, length:%d, message:%s", this.index, pm.Topic, pm.Partition, pm.Offset, length, msg)
		}
	}()

	this.producer = producer

	return nil
}

func (this *KafkaSaver) send(p *common.Packet) error {
	var err error
	if this.producer == nil {
		err = this.connect()
		if err != nil {
			return err
		}
		this.logger.Info("connect to kafka success")
	}	

	this.producer.Input() <- &sarama.ProducerMessage{
		Topic: this.topic,
		Value: p,
	}

	return nil
}

//func (this *KafkaSaver) testConsumer() {
//	this.logger.Debug("test consumer")
//	config := sarama.NewConfig()
//	this.logger.Debug("kafka config:%v", config)
//	cli, err := sarama.NewClient(this.brokers, config)
//	if err != nil {
//		this.logger.Debug("new client failed, error:%s", err.Error())
//		return
//	}
//	topics, err := cli.Topics()
//	if err != nil {
//		this.logger.Debug("get client topics failed, error:%s", err.Error())
//		return
//	}
//	this.logger.Debug("client topics:%v", topics)
//	partitions, err := cli.Partitions(this.topic)
//	if err != nil {
//		this.logger.Debug("get client partitions failed, error:%s", err.Error())
//		return
//	}
//	this.logger.Debug("client partitions:%v", partitions)
//	leader, err := cli.Leader(this.topic, partitions[0])
//	if err != nil {
//		this.logger.Debug("get client partition%d leader failed, error:%s", partitions[0], err.Error())
//		return
//	}
//	this.logger.Debug("client partition %d leader:%v", partitions[0], leader)
//	
//	consumer, err := sarama.NewConsumerFromClient(cli)
//	if err != nil {
//		this.logger.Debug("get client consumer failed, error:%s", err.Error())
//		return
//	}
//	this.logger.Debug("client consumer:%v", consumer)
//
//	pc, err := consumer.ConsumePartition(this.topic, partitions[0], sarama.OffsetNewest)
//	if err != nil {
//		this.logger.Debug("get client partition consumer failed, error:%s", err.Error())
//		return
//	}
//	this.logger.Debug("client partition consumer:%v", pc)
//	
//	for {
//		msg := <- pc.Messages()
//		this.logger.Debug("get message success, key:%s, value:%s, topic:%s, partition:%d, offset:%d", string(msg.Key), string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
//		time.Sleep(time.Second)
//	}
//
//}

func (this *KafkaSaver) Stop() {
	this.running = false
	this.logger.Info("KafkaSaver%d stop", this.index)
}
