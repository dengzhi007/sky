package main

import (
	//basic
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	//third-party lib
	"lib/go-config/config"

	//project package
	"common"
	"network"
)

const (
	VERSION string = "transfer_v4.0"
)

func initialize() error {
	fmt.Println("initilize ...")

	confFile := flag.String("c", "./etc/transfer.conf", "config file name")
	if confFile == nil {
		return fmt.Errorf("no config file")
	}
	flag.Parse()

	fmt.Printf("load configuration:%s\n", *confFile)
	conf, err := config.ReadDefault(*confFile)
	if err != nil {
		return fmt.Errorf("load config file failed:%s\n", err.Error())
	}
	common.Conf = conf

	max_processor := common.GetConfInt("global", "max_processor", runtime.NumCPU())
	runtime.GOMAXPROCS(max_processor)

	dir, err := common.Conf.String("global", "root_dir") 
	if err == nil {
		//workding directory is resigned, change to that...
		err = os.Chdir(dir)
		if err != nil {
			return fmt.Errorf("change working directory to %s failed:%s\n", dir, err.Error())
		}
	}
	common.Dir, _ = os.Getwd()
	
	num := common.GetConfInt("global", "worker_num", 1)
	if num < 1 {
		return fmt.Errorf("work number must be larger than 1")
	}

	common.WorkerNum = num
	common.PacketChans = make([]chan []byte, num)
	for i := 0; i < num; i++ {
		common.PacketChans[i] = make(chan []byte, common.GetConfInt("server", "packet_chan_size", 10000))
		if common.PacketChans[i] == nil {
			return fmt.Errorf("make packet channel failed")
		}
	}
	
	fmt.Println("initilize over")
	fmt.Printf("Program %s start success in %s at: %s, Max processor:%d Worker number:%d\n", VERSION, common.Dir, time.Now(), max_processor, num)
	common.Alarm(VERSION + "-start")

	return nil
}

func main() {
	if err := initialize(); err != nil {
		panic(fmt.Sprintf("initialize failed:%s\n", err.Error()))
	}

	//start server...
	server := network.NewServer()
	if server == nil {
		panic("new tcp server failed.")
	}
	go server.Start()


	//start worker
	backends := make([]*network.Backend, common.WorkerNum)
	for i := 0; i < common.WorkerNum; i++ {
		backends[i] = network.NewBackend(i, common.PacketChans[i])
		if backends[i] == nil {
			panic("new backend failed.")
		}
		go backends[i].Start()
	}


	fmt.Printf("Program %s start success, wait for quit signal...\n", VERSION)

	//wait for signal
	sig_chan := make(chan os.Signal)
	signal.Notify(sig_chan, os.Interrupt, syscall.SIGTERM)
	<-sig_chan

	//clear works...
	server.Stop()
	for i := 0; i < common.WorkerNum; i++ {
		backends[i].Stop()
	}

	common.Alarm(VERSION + "-exit")
	time.Sleep(time.Second)
	fmt.Printf("Program %s quit success at: %s\n", VERSION, time.Now())
}
