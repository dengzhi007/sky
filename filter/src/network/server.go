package network

import (
	"fmt"
	"strconv"
	"net"
	"common"
	"time"
	"lib/log4go"
	"encoding/json"
	"sync"
)

type Server struct {
	ip            string  // "" bind all interface
	port          int     // 8080
	maxClients    int     // 100000 concurrent connections
	clientNum     int     // initial 0
	acceptTimeout time.Duration
	connTimeout   time.Duration     // 3 min, Duration is nanosecond 10^9
	headerLen     int
	maxBodyLen    int     // 100k bytes
	running       bool    // false
	logger        log4go.Logger
	heartbeat     int64
	workerNum     int
	currentWorker int
	lock          sync.Mutex
}

func NewServer() *Server {
	s := &Server{
		port: common.GetConfInt("server", "port", 8080),
		maxClients: common.GetConfInt("server", "max_clients", 100000),
		clientNum: 0,
		acceptTimeout: common.GetConfSecond("server", "accept_timeout", 60*5),
		connTimeout: common.GetConfSecond("server", "connection_timeout", 60*3),
		headerLen: common.GetConfInt("server", "header_length", 10),
		maxBodyLen: common.GetConfInt("server", "max_body_length", 102400),
		running: false,
		heartbeat: time.Now().Unix(),
		workerNum: common.WorkerNum,
		currentWorker: -1,
		logger: common.NewLogger("server"),
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
		ip = ""
	}
	s.ip = ip

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
	this.logger.Info("Accept connection from %s\n", addr)	

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
		this.logger.Info("Read from %s, Length: %d, Content: %s\n", addr, num, string(data))	
		
		//generate packets and put to channel for later filtering
		err = this.output(data, outChan)
		if err != nil {
			this.logger.Error("Output data to packet channel failed, addr: %s, error:%s", addr, err.Error())	
			break
		}
	}
	
	this.lock.Lock()
	this.clientNum -= 1
	this.lock.Unlock()

	this.logger.Info("Connection %s closed\n", addr)	
}

func (this *Server) receive(buf []byte, conn net.Conn) ([]byte, error) {
	conn.SetDeadline(time.Now().Add(this.connTimeout))
	readNum := 0
	length := len(buf)

	var num int
	var err error
	for readNum < length {
		num, err = conn.Read(buf[ readNum : ])
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
                return fmt.Errorf("host is not a string")
	}

        //time, exists := m["time"]
        //if !exists {
        //      fmt.Printf("json unmarshal from %s failed, time not exists", s)
        //      return nil
        //}

        datas, exists := m["data"]
        if !exists {
                return fmt.Errorf("data not exists")
        }
	items, ok := datas.([]interface{})
	if !ok {
                return fmt.Errorf("data is not []interface")
	}
        length := len(items)
        if length < 1 {
                return fmt.Errorf("data item length < 1")
        }

        for i := 0; i < length; i++ {
		item, ok := items[i].([]interface{})
		if !ok {
			return fmt.Errorf("data item is not []interface{}")
		}
		item_id, ok := item[0].(string)
		if !ok {
                	return fmt.Errorf("item id is not a string")
		}
		value, ok := item[1].(string)
		if !ok {
                	return fmt.Errorf("value is not a string")
		}
		itemId, _ := strconv.Atoi(item_id)
		val, _ := strconv.ParseFloat(value, 64)

		//insert into out channel
		select {
			case outChan <- common.NewPacket(hostname, itemId, val):
				this.logger.Debug("Insert into packet channel success, hostname:%s, item_id:%s, value:%s", hostname, item_id, value)
			default:
				this.logger.Error("Insert into packet channel timeout, channel length:%d too more, drop packet: hostname:%s, item id:%s, value:%s", len(outChan), hostname, item_id, value)
				common.Alarm("packet_channel_timeout")
				time.Sleep(time.Second)
				return fmt.Errorf("channel timeout")
		}
        }

	this.logger.Info("Put %d alarm packets into packet channel", length)	
	return nil
}

func (this *Server) Stop() {
	this.running = false
	this.logger.Info("server stop")
}


