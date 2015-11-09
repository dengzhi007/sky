package network

import (
	"common"
	"fmt"
	"lib/log4go"
	"net"
	"strconv"
	"sync"
	"os"
	"time"
	"syscall"
)

type Server struct {
	ip            string // "" bind all interface
	port          int    // 8080
	maxClients    int    // 100000 concurrent connections
	clientNum     int    // initial 0
	acceptTimeout time.Duration
	connTimeout   time.Duration // 3 min, Duration is nanosecond 10^9
	slowread      time.Duration
	headerLen     int  // 10
	maxBodyLen    int  // 100k bytes
	running       bool // false
	logger        log4go.Logger
	heartbeat     int64
	workerNum     int
	currentWorker int
	lock          sync.Mutex
}

func NewServer() *Server {
	s := &Server{
		port:          common.GetConfInt("server", "port", 8080),
		maxClients:    common.GetConfInt("server", "max_clients", 100000),
		clientNum:     0,
		acceptTimeout: common.GetConfSecond("server", "accept_timeout", 60*5),
		connTimeout:   common.GetConfSecond("server", "connection_timeout", 60*3),
		headerLen:     common.GetConfInt("server", "header_length", 10),
		maxBodyLen:    common.GetConfInt("server", "max_body_length", 102400),
		running:       false,
		heartbeat:     time.Now().Unix(),
		workerNum:     common.WorkerNum,
		currentWorker: -1,
		slowread:      common.GetConfSecond("server", "slow_read", 0),
		logger:        common.NewLogger("server"),
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

	max_clients := uint64(s.maxClients)
	if max_clients > 1024 {
		var rlim syscall.Rlimit
		err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
		if err != nil {
			fmt.Println("server get rlimit error: " + err.Error())
			return nil
		}
		rlim.Cur = max_clients
		rlim.Max = max_clients
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
		if err != nil {
			fmt.Println("server set rlimit error: " + err.Error())
			return nil
		}
		s.logger.Info("set fd limit to %d", s.maxClients)
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
	//autoset: reuseaddr nodelay nonblock
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
			this.logger.Error("Tcp server accept connection:%v failed:%s", conn, err.Error())
			this.logger.Info("Server master cron total connections number: %d", this.clientNum)
			for i := 0; i < this.workerNum; i++ {
				this.logger.Info("Server master cron output channel%d length: %d", i, len(common.PacketChans[i]))
			}
			if conn != nil {
				conn.Close()
				time.Sleep(time.Second)
			}
			continue
		}

		if this.clientNum >= this.maxClients-1 {
			this.logger.Error("Client number now is %d, exceed maxClients:%d, connection close, remote addr:%s", this.clientNum+1, this.maxClients, conn.RemoteAddr().String())
			conn.Close()
			this.running = false
			common.Alarm("fd_full_auto_restart")
			time.Sleep(time.Second*3)

			os.Exit(1)
		}

		this.lock.Lock()
		this.clientNum += 1
		this.lock.Unlock()
		
		go this.process(conn)
	}

}

func (this *Server) process(conn net.Conn) {
	if conn == nil {
		this.logger.Error("input conn param is nil")
		return
	}
	defer conn.Close()

	headerNum := this.headerLen
	buf := make([]byte, headerNum+this.maxBodyLen)

	var (
		num  int
		data []byte
		err  error
		echo []byte = []byte("ok")
	)

	this.currentWorker = (this.currentWorker + 1) % this.workerNum
	worker := this.currentWorker
	outChan := common.PacketChans[worker]

	addr := conn.RemoteAddr().String()
	this.logger.Info("Accept connection from %s, total clients number now:%d", addr, this.clientNum)

	for this.running {
		if this.slowread > 0 {
			time.Sleep(this.slowread)
		}

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

		// ack ok back
		err = this.send(echo, conn)
		if err != nil {
			this.logger.Error("Echo back %s to %s failed, error:%s", string(echo), addr, err.Error())
			break
		}

		data = make([]byte, num)
		copy(data, buf[0:num])
		select {
		case outChan <- data:
			this.logger.Debug("Insert into output channel success, bytes:%s", string(data))
		default:
			this.logger.Error("Insert into output channel failed, channel length:%d too more, drop packet: %s", len(outChan), string(data))
			common.Alarm(fmt.Sprintf("packet_channel%d_full", worker))
			time.Sleep(time.Second)
			break
		}
	}

	this.lock.Lock()
	this.clientNum -= 1
	this.lock.Unlock()

	this.logger.Info("connection %s closed, remaining total clients number: %d", addr, this.clientNum)
}

func (this *Server) receive(buf []byte, conn net.Conn) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(this.connTimeout))
	readNum := 0
	length := len(buf)

	var num int
	var err error
	for readNum < length {
		num, err = conn.Read(buf[readNum:])
		if err != nil {
			this.logger.Debug("read conn error: %s, close connection, already read %d bytes", err.Error(), readNum)
			return nil, err
		}
		this.logger.Debug("Read %d bytes: %s", num, string(buf))
		readNum += num
	}

	return buf, nil
}

func (this *Server) send(buf []byte, conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(this.connTimeout))
	num, err := conn.Write(buf)
	if err != nil {
		return err
	}

	if num <= 0 {
		return fmt.Errorf("connection write back %d bytes error", num)
	}

	return nil
}

func (this *Server) Stop() {
	this.running = false
	this.logger.Info("server stop")
}
