package common

import (
	"lib/go-config/config"
	"net/http"
	"net/url"
	"time"
	log "lib/log4go"
	"fmt"
)

//root directory of this application
var Dir string

var Conf *config.Config
var WorkerNum int

//channel for packets from network outside
var PacketChans []chan *Packet

//channel for filtered packets from PacketChan
var FilterChans []chan []byte

//string key: hostname_itemId, int key: ruleId
var Rules map[string]map[int]*Rule

var lastAlarmTime int64



//---------------------------tools-----------------------------------------//
func GetConfInt(section string, key string, def int) int {
	if Conf == nil {
		return def
	}

	val, err := Conf.Int(section, key)
        if err != nil {
		return def
        }

	return val
}

func GetConfSecond(section string, key string, def time.Duration) time.Duration {
	def *= time.Second
	if Conf == nil {
		return def
	}

	val, err := Conf.Int(section, key)
        if err != nil {
		return def
        }

	return time.Duration(val) * time.Second
}

func NewLogger(name string) log.Logger {
        logFileName := Dir + "/logs/" + name + ".log"
        flw := log.NewFileLogWriter(logFileName, true)
        flw.SetRotateDaily(true)

        level := log.INFO
        if Conf != nil {
                val, err := Conf.String("global", "log_level")
                if err == nil {
                        switch val {
                        case "info":
                                level = log.INFO
                        case "debug":
                                level = log.DEBUG
                        case "error":
                                level = log.ERROR
                        }
                }
        }

        l := make(log.Logger)
        l.AddFilter("log", level, flw)

        return l
}


func Alarm(subject string) {
	if Conf == nil {
		return
	}
	
	now := time.Now().Unix()
	interval := GetConfInt("global", "alarm_interval", 300)
	if now-lastAlarmTime < int64(interval) {
		return
	}
	lastAlarmTime = now

	alarmUrl, err := Conf.String("global", "alarm_url")
	if err != nil {
		return
	}
	client := &http.Client{
		Timeout: 10*time.Second,
	}
	
	url := fmt.Sprintf(alarmUrl, url.QueryEscape(subject))
        fmt.Println("alarm " + url)
	_, err = client.Get(url)
	if err != nil {
        	fmt.Println("alarm failed " + err.Error())
	}
}
