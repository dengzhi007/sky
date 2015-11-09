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
	"task"
)

const (
	VERSION string = "kafka-saver_v2.0"
)

func initialize() error {
	fmt.Println("initilize ...")

	confFile := flag.String("c", "./etc/kafka-saver.conf", "config file name")
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
	common.PacketChans = make([]chan *common.Packet, num)
	for i := 0; i < num; i++ {
		common.PacketChans[i] = make(chan *common.Packet, common.GetConfInt("server", "packet_chan_size", 100000))
	}

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
	savers := make([]*task.KafkaSaver, common.WorkerNum)
	logger := common.NewLogger("kafka")
	for i := 0; i < common.WorkerNum; i++ {
		savers[i] = task.NewKafkaSaver(i, common.PacketChans[i], logger)
		if savers[i] == nil {
			panic("new filter failed.")
		}
		go savers[i].Start()
	}

	fmt.Printf("Program %s start success, wait for quit signal...\n", VERSION)
	//wait for signal
	sig_chan := make(chan os.Signal)
	signal.Notify(sig_chan, os.Interrupt, syscall.SIGTERM)
	<-sig_chan

	//clear works...
	server.Stop()
	for i := 0; i < common.WorkerNum; i++ {
		savers[i].Stop()
	}

	common.Alarm(VERSION + "-exit")
	time.Sleep(time.Second)
	fmt.Printf("Program %s quit success at: %s\n", VERSION, time.Now())
}
