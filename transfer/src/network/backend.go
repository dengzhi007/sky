package network

import (
	"common"
	"fmt"
	"io"
	log "lib/log4go"
	"net"
	"strings"
	"time"
)

type Backpoint struct {
	name           string   // ff alarm redis_saver mongo_saver hbase_saver
	backNum        int      // one backpoint may deploy many back machines for high availability
	backAddrs      []string // send to one of this
	currentBackIdx int
	retryTimes     int
	retryInterval  time.Duration
	conn           net.Conn
	connTimeout    time.Duration
	sendTimeout    time.Duration
	recvBuf        []byte
	sendingChan    chan []byte
}

type Backend struct {
	index       int          // worker index
	backendNum  int          // backend number: more than one backend to send
	backendList []*Backpoint // send to all of this
	heartbeat   int64
	logger      log.Logger
	inChan      chan []byte
	running     bool
}

func NewBackend(idx int, inputChan chan []byte) *Backend {
	b := &Backend{
		index:      idx,
		backendNum: 0,
		heartbeat:  time.Now().Unix(),
		logger:     common.NewLogger("backend"),
		inChan:     inputChan,
		running:    false,
	}

	if b == nil {
		fmt.Println("new backend failed")
		return nil
	}

	if b.logger == nil {
		fmt.Println("backend new logger failed")
		return nil
	}

	if b.inChan == nil {
		fmt.Println("backend must has an input channel to get bytes for back point")
		return nil
	}

	options, err := common.Conf.Options("backend")
	if err != nil {
		fmt.Printf("backend get all options failed:%s\n", err.Error())
		return nil
	}

	b.backendList = make([]*Backpoint, 0, 4)
	for _, option := range options {
		if !strings.HasPrefix(option, "backend_list_") {
			continue
		}

		back, err := common.Conf.String("backend", option)
		if err != nil {
			fmt.Printf("Read conf %s failed, error: %s, get backend total number:%d\n", option, err.Error(), b.backendNum)
			break
		}

		backend_name := strings.TrimPrefix(option, "backend_list_")
		if backend_name == "" || backend_name == option {
			fmt.Printf("get backend name failed")
			break
		}

		addrs := strings.Split(back, ";")
		num := len(addrs)

		if num < 1 {
			fmt.Printf("one backend:%s must at least has one address", backend_name)
			continue
		}

		point := &Backpoint{
			name:           backend_name,
			backNum:        num,
			backAddrs:      addrs,
			conn:           nil,
			connTimeout:    common.GetConfSecond("backend", "connection_timeout", 180),
			sendTimeout:    common.GetConfSecond("backend", "send_timeout", 180),
			currentBackIdx: -1,
			retryTimes:     common.GetConfInt("backend", "retry_times", 5),
			retryInterval:  common.GetConfSecond("backend", "retry_interval", 5),
			recvBuf:        make([]byte, common.GetConfInt("backend", "receive_buffer_size", 10)),
			sendingChan:    make(chan []byte, common.GetConfInt("backend", "sending_buffer_size", 10000)),
		}
		if point == nil {
			fmt.Println("new back point failed")
			return nil
		}

		b.backendList = append(b.backendList, point)
		b.backendNum += 1
		b.logger.Debug("Backend%d get a back %s: %s, %d points", idx, backend_name, back, num)
	}

	if b.backendNum < 1 {
		fmt.Println("no backends")
		return nil
	}

	return b
}

func (this *Backend) Start() {
	this.running = true
	this.logger.Info("Backend%d start to work, wait for dispatching...", this.index)

	var (
		bytes []byte
		ok    bool
		point *Backpoint
	)

	for _, point = range this.backendList {
		go this.sending(point)
	}

	for this.running {
		this.heartbeat = time.Now().Unix()

		bytes, ok = <-this.inChan
		if !ok {
			this.logger.Error("Backend%d read from filter channel failed", this.index)
			break
		}
		this.logger.Debug("Backend%d get data from input channel success Length:%d, Content: %s", this.index, len(bytes), string(bytes))

		for _, point = range this.backendList {
			select {
			case point.sendingChan <- bytes:
			default:
				this.logger.Error("Backend%d insert into backpoint %s sending channel failed, length:%d too more, data dropped:%s", this.index, point.name, len(point.sendingChan), string(bytes))
				common.Alarm(fmt.Sprintf("backend%d_%s_sendingchan_block", this.index, point.name))
			}
		}

		this.logger.Debug("Backend%d dispatch out data to all back %d points success, remaining %d packets in packet channel", this.index, this.backendNum, len(this.inChan))
	}

	this.logger.Info("Backend%d quit working", this.index)
}

func (this *Backpoint) connect() error {
	if this.currentBackIdx < 0 || this.currentBackIdx >= this.backNum {
		this.currentBackIdx = time.Now().Nanosecond() % this.backNum
	}

	var addr string
	var conn net.Conn
	var err error

	for i := 0; i < this.retryTimes; i++ {
		this.currentBackIdx = (this.currentBackIdx + 1) % this.backNum
		addr = this.backAddrs[this.currentBackIdx]
		conn, err = net.DialTimeout("tcp", addr, this.connTimeout)
		if err == nil {
			this.conn = conn
			return nil
		}
		if conn != nil {
			conn.Close()
		}

		time.Sleep(this.retryInterval)
	}

	this.conn = nil
	common.Alarm(this.name + "-down")
	return err
}

func (this *Backend) sending(point *Backpoint) {
	var bytes []byte
	var ok bool
	var err error

	this.logger.Info("Backend%d back point %s start to work, wait for sending...", this.index, point.name)

	for this.running {
		bytes, ok = <-point.sendingChan
		if !ok {
			this.logger.Error("Backend%d backpoint %s read from sending channel failed", this.index, point.name)
			break
		}
		this.logger.Debug("Backend%d backpoint %s read from sending channel success:%s", this.index, point.name, string(bytes))

		err = this.send(bytes, point)
		if err != nil {
			this.logger.Error("Backend%d send data out to %s failed, error: %s, data dropped, remaining %d packets in sending channel: %s", this.index, point.name, err.Error(), len(point.sendingChan), string(bytes))
			if point.conn != nil {
				point.conn.Close()
				point.conn = nil
			}
			time.Sleep(point.retryInterval)
			continue
		}

		this.logger.Debug("Backend%d send data out to %s success, remaining %d packets in sending channel", this.index, point.name, len(point.sendingChan))
	}

	if point.conn != nil {
		this.logger.Info("Backend%d back point %s end working", this.index, point.name)
		point.conn.Close()
	}

	this.logger.Info("Backend%d back point %s quit working", this.index, point.name)
}

func (this *Backend) send(bytes []byte, point *Backpoint) error {
	var err error
	var num int
	if point.conn == nil {
		err = point.connect()
		if err != nil {
			this.logger.Error("Backend%d connect to %s failed", this.index, point.name)
			return err
		}
		this.logger.Info("Backend%d connect to %s success, IP:%s", this.index, point.name, point.backAddrs[point.currentBackIdx])
	}

	length := len(bytes)
	sentNum := 0
	start := time.Now()

	// it is no need to detect whether is closed by peer, if is closed, Write() will return an "EOF" error
	point.conn.SetWriteDeadline(time.Now().Add(point.sendTimeout))
	for sentNum < length {
		num, err = point.conn.Write(bytes[sentNum:])
		if err != nil {
			this.logger.Error("Backend%d write to %s failed, duration %v, error:%s, IP:%s, yet already sent %d bytes", this.index, point.name, time.Now().Sub(start), err.Error(), point.backAddrs[point.currentBackIdx], sentNum)
			return err
		}
		sentNum += num
	}
	this.logger.Debug("Backend%d write to %s success, duration %v, IP:%s, Length:%d, Content:%s", this.index, point.name, time.Now().Sub(start), point.backAddrs[point.currentBackIdx], sentNum, string(bytes))

	point.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
	num, err = point.conn.Read(point.recvBuf)
	if err != nil {
		this.logger.Error("Backend%d read from %s failed, IP:%s, error:%s", this.index, point.name, point.backAddrs[point.currentBackIdx], err.Error())
		if err == io.EOF {
			this.logger.Error("Backend%d detect close packet from %s, IP:%s", this.index, point.name, point.backAddrs[point.currentBackIdx])
			//closed by peer
			return err
		}
	}
	if num > 0 {
		this.logger.Info("Backend%d read from %s success, IP:%s, Length:%d, Content:%s", this.index, point.name, point.backAddrs[point.currentBackIdx], num, string(point.recvBuf[0:num]))
	}

	return nil
}

func (this *Backend) Stop() {
	this.running = false
	this.logger.Info("Backend%d stop", this.index)
}
