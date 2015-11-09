package network

import (
	"common"
	"encoding/json"
	"fmt"
	"lib/log4go"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"reflect"
)

type Server struct {
	ip            string // "" bind all interface
	port          int    // 8080
	maxClients    int    // 100000 concurrent connections
	clientNum     int    // initial 0
	headerLen     int    // initial 10
	maxBodyLen    int    // 100k bytes
	workerNum     int
	currentWorker int
	running       bool
	heartbeat     int64
	logger        log4go.Logger
	lock          sync.Mutex
	acceptTimeout time.Duration   // 3 min, for master cron to statistic
	connTimeout   time.Duration   // 3 min, Duration is nanosecond 10^9
	ipWhitelist   map[string]bool // ip:true
	itemWhitelist map[int]string  // itemId:itemName
	itemBlacklist map[int]string
}

func NewServer() *Server {
	s := &Server{
		port:          common.GetConfInt("server", "port", 8080),
		maxClients:    common.GetConfInt("server", "max_clients", 100000),
		headerLen:     common.GetConfInt("server", "header_length", 10),
		maxBodyLen:    common.GetConfInt("server", "max_body_length", 102400),
		acceptTimeout: common.GetConfSecond("server", "accept_timeout", 60*5),
		connTimeout:   common.GetConfSecond("server", "connection_timeout", 60*3),
		clientNum:     0,
		currentWorker: -1,
		running:       false,
		heartbeat:     time.Now().Unix(),
		workerNum:     common.WorkerNum,
		logger:        common.NewLogger("server"),
		ipWhitelist:   nil,
		itemWhitelist: nil,
		itemBlacklist: nil,
	}

	if s == nil {
		fmt.Println("new server failed")
		return nil
	}

	if s.logger == nil {
		fmt.Println("new server logger failed")
		return nil
	}

	ip, err := common.Conf.String("server", "bind")
	if err != nil {
		//not set, bind all interface
		ip = ""
	}
	s.ip = ip

	white_list, err := common.Conf.String("server", "ip_whitelist")
	if err == nil {
		s.ipWhitelist = make(map[string]bool)
		for _, ip := range strings.Split(white_list, ";") {
			s.ipWhitelist[ip] = true
		}
	}
	s.logger.Debug("connection ip white list:%v", s.ipWhitelist)

	options, err := common.Conf.Options("item_blacklist")
        if err == nil && len(options) > 0 {
		s.itemBlacklist = make(map[int]string)
		for _, option := range options {
			itemName, err := common.Conf.String("item_blacklist", option)
			if err != nil {
				continue
			}
			itemId, _ := strconv.Atoi(option)
			s.itemBlacklist[itemId] = itemName
		}
		s.logger.Debug("packet item black list:%v", s.itemBlacklist)
        }

	options, err = common.Conf.Options("item_whitelist")
        if err == nil && len(options) > 0 {
		s.itemWhitelist = make(map[int]string)
		for _, option := range options {
			itemName, err := common.Conf.String("item_whitelist", option)
			if err != nil {
				continue
			}
			itemId, _ := strconv.Atoi(option)
			s.itemWhitelist[itemId] = itemName
		}
		s.logger.Debug("packet item white list:%v", s.itemWhitelist)
        }

	return s
}

func (this *Server) Start() {
	addr := fmt.Sprintf("%s:%d", this.ip, this.port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		this.logger.Error("tcp server resolve address %s failed:%s", addr, err.Error())
		return
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		this.logger.Error("tcp server listen on %s failed:%s", addr, err.Error())
		return
	}
	defer listener.Close()

	this.running = true
	this.currentWorker = time.Now().Nanosecond() % this.workerNum
	this.logger.Info("tcp server start to listen on %s", addr)

	for this.running {
		this.heartbeat = time.Now().Unix()

		listener.SetDeadline(time.Now().Add(this.acceptTimeout))
		conn, err := listener.Accept()
		if err != nil {
			this.logger.Error("tcp server accept failed:%s", err.Error())
			this.logger.Info("Server master cron total connections number: %d", this.clientNum)
			for i := 0; i < this.workerNum; i++ {
				this.logger.Info("Server master cron output channel%d length: %d", i, len(common.PacketChans[i]))
			}
			continue
		}

		if this.ipWhitelist != nil {
			ip := strings.Split(conn.RemoteAddr().String(), ":")[0]
			if _, ok := this.ipWhitelist[ip]; !ok {
				this.logger.Error("remote ip:%s not in ipWhitelist", ip)
				conn.Close()
				continue
			}
		}

		if this.clientNum > this.maxClients-1 {
			this.logger.Error("client num now is %d, exceed maxClients:%d, connection close, remote address:%s", this.clientNum+1, this.maxClients, conn.RemoteAddr().String())
			conn.Close()
			time.Sleep(time.Second)
			continue
		}

		this.lock.Lock()
		this.clientNum += 1
		this.lock.Unlock()

		go this.process(conn)
	}

}

func (this *Server) process(conn net.Conn) {
	defer conn.Close()

	headerNum := this.headerLen
	buf := make([]byte, headerNum+this.maxBodyLen)

	var num int
	var data []byte
	var err error

	this.currentWorker = (this.currentWorker + 1) % this.workerNum
	outChan := common.PacketChans[this.currentWorker]

	addr := conn.RemoteAddr().String()
	this.logger.Info("Accept connection from %s", addr)

	for this.running {
		//header
		data, err = this.receive(buf[0:headerNum], conn)
		if err != nil {
			this.logger.Error("Read header from %s failed, error:%s", addr, err.Error())
			break
		}

		num, err = strconv.Atoi(string(data))
		if err != nil || num <= 0 || num > this.maxBodyLen {
			this.logger.Error("Read header from %s format error, header content: %s", addr, string(data))
			break
		}

		//body
		num += headerNum
		data, err = this.receive(buf[headerNum:num], conn)
		if err != nil {
			this.logger.Error("Read body from %s failed, error:%s", addr, err.Error())
			break
		}

		//read success
		this.logger.Info("Read from %s, Length: %d, Content: %s", addr, num, string(data))

		//generate packets and put to channel
		err = this.output(data, outChan)
		if err != nil {
			this.logger.Error("Output data to packet channel failed, addr: %s, error:%s", addr, err.Error())
			break
		}
	}

	this.lock.Lock()
	this.clientNum -= 1
	this.lock.Unlock()

	this.logger.Info("Connection %s closed", addr)
}

func (this *Server) receive(buf []byte, conn net.Conn) ([]byte, error) {
	conn.SetDeadline(time.Now().Add(this.connTimeout))
	readNum := 0
	length := len(buf)

	var num int
	var err error
	for readNum < length {
		num, err = conn.Read(buf[readNum:])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				time.Sleep(time.Second)
				continue
			}
			return nil, err
		}
		this.logger.Debug("Read %d bytes: %s", num, string(buf))
		readNum += num
	}

	_, err = conn.Write([]byte("ok"))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (this *Server) output(data []byte, outChan chan *common.Packet) error {
	var err error

	m := make(map[string]interface{})
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	host, exists := m["host"]
	if !exists {
		return fmt.Errorf("host not exists")
	}
	hostname, ok := host.(string)
	if !ok {
		return fmt.Errorf("host is not a string, type:%v, host:%v", reflect.TypeOf(host), host)
	}

	//agenttime localtime
	agenttime, exists := m["time"]
	if !exists {
		return fmt.Errorf("time not exists")
	}
	agenttime_s, ok := agenttime.(string)
	if !ok {
		return fmt.Errorf("time is not a string, type:%v, time:%v", reflect.TypeOf(agenttime), agenttime)
	}
	agent_time, _ := strconv.ParseInt(agenttime_s, 10, 64)

	datas, exists := m["data"]
	if !exists {
		return fmt.Errorf("data not exists")
	}
	items, ok := datas.([]interface{})
	if !ok {
		return fmt.Errorf("data is not array: []interface, type:%v, data:%v", reflect.TypeOf(datas), datas)
	}
	length := len(items)
	if length < 1 {
		return fmt.Errorf("data items length < 1")
	}

	p := common.NewPacket(hostname, agent_time)
	var itemName string
	for i := 0; i < length; i++ {
		//item:  [item_id, value, val_type]
		item, ok := items[i].([]interface{})
		if !ok {
			return fmt.Errorf("data item is not array []interface, type:%v, item:%v", reflect.TypeOf(items[i]), items[i])
		}
		if len(item) < 3 {
			return fmt.Errorf("data item length < 3, item:%v", item)
		}
		item_id, ok := item[0].(string)
		if !ok {
			return fmt.Errorf("item id is not string, type:%v, id:%v", reflect.TypeOf(item[0]), item[0])
		}
		itemId, _ := strconv.Atoi(item_id)
		value, ok := item[1].(string)
		if !ok {
			return fmt.Errorf("value is not string, value:%v", item[1])
		}

		if this.itemBlacklist != nil {
			if _, ok = this.itemBlacklist[itemId]; ok {
				this.logger.Debug("item id:%d is in item blacklist, drop", itemId)
				continue
			}
		}

		if this.itemWhitelist != nil {
			if itemName, ok = this.itemWhitelist[itemId]; !ok {
				this.logger.Debug("item id:%d not in item whitelist, drop", itemId)
				continue
			}
		}

		p.AddItem(itemId, value, itemName)
	}

	if p.ItemNum < 1 {
		this.logger.Debug("packet item num < 1, hostname:%s, time:%s", hostname, agenttime_s)
		return nil
	}

	//insert into output channel
	select {
	case outChan <- p:
		this.logger.Debug("Insert into packet channel success: Hostname:%s, time:%s", hostname, agenttime_s)
	default:
		this.logger.Error("Insert into packet channel failed, channel length:%d too more, drop packet: %v", len(outChan), p)
		common.Alarm("packet_channel_block")
		time.Sleep(time.Second)
		return fmt.Errorf("packet channel block")
	}

	this.logger.Info("Put %d packet items into packet channel", p.ItemNum)

	return nil
}

func (this *Server) Stop() {
	this.running = false
	this.logger.Info("server stop")
}
