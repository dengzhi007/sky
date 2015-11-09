package task

import (
	"common"
	log "lib/log4go"
	"time"
	"fmt"
	"strings"
	"strconv"
	"lib/redis"
)


type Updater struct {
	running      bool
	heartbeat    int64
	redisCli     redis.Client
	rules        map[string]map[int]*common.Rule  // string key: hostname_itemId, int key: ruleId
	logger       log.Logger
	interval     time.Duration    // 20min 
}

func NewUpdater() *Updater {
	u := &Updater{
		running: false,
		heartbeat: time.Now().Unix(),
		interval: common.GetConfSecond("updater", "interval", 20*60),
		logger: common.NewLogger("updater"),
	}
	
	if u == nil {
                fmt.Println("new updater failed")
                return nil
	}

	redisAddr, err := common.Conf.String("updater", "redis_addr")
	if err != nil {
                fmt.Printf("updater get redis_addr failed, err:%s\n", err.Error())
                return nil
	}
	u.redisCli.Addr = redisAddr
	u.redisCli.Db = common.GetConfInt("updater", "redis_db", 0)

        if u.logger == nil {
                fmt.Println("updater new logger failed")
                return nil
        }

	return u
}

func (this *Updater) Start() {
	this.running = true
	this.logger.Info("Updater start to work")

	for this.running {
		this.heartbeat = time.Now().Unix()

		this.updateRule()

		time.Sleep(this.interval)
	}
}

func (this *Updater) updateRule() {

	this.rules = make(map[string]map[int]*common.Rule)
	//this must get max version	
	verset, err := this.redisCli.Smembers("versions_set")
	if err != nil {
		this.logger.Error("Get rule redis max version set failed:%s", err.Error())
		return
	}
	
	var version int = 0
	for _, v := range verset {
		t, err := strconv.Atoi(string(v))
		if err != nil {
			this.logger.Error("Convert verset version to int failed:%s", err.Error())
			return
		}
		if t > version {
			version = t
		}
		
	}
	
	rules, err := this.redisCli.Lrange(fmt.Sprintf("%d", version), 0, -1)
	if err != nil {
		this.logger.Error("Lrange rule redis current version:%d failed:%s", version, err.Error())
		return
	}
	
	length := len(rules)
	if length == 0 {
		this.logger.Error("Lrange rule redis current version:%d length is 0", version)
		return
	}
	
	this.logger.Info("Updater start to update rules from redis current version: %d, rule count: %d...", version, length)

	var items []string	
	var op string
	var hostname string
	var rule_id int
	var item_id int
	var compare_type string
	var threshold float64
	
	var key string // hostname_itemId
	var rule string

	for _, v := range rules {
		rule = string(v)
		this.logger.Debug("Read redis rule: %s", rule)
		items = strings.Split(rule, "\x01#")
		if len(items) < 16 {
			this.logger.Error("Redis rule items format error, length must not less than 14, now is %d", len(items))
			continue
		}
		
		op = strings.ToUpper(items[0])
		hostname = items[1]
		rule_id, _ = strconv.Atoi(items[2])
		item_id, _ = strconv.Atoi(items[4])
		compare_type = items[6]
		threshold, _ = strconv.ParseFloat(items[7], 64)
	
		key = fmt.Sprintf("%s_%d", hostname, item_id)
		if _, ok := this.rules[key]; !ok {
			this.rules[key] = make(map[int]*common.Rule)	
		}
		if op == "A" {
			this.rules[key][rule_id] = common.NewRule(hostname, rule_id, item_id, compare_type, threshold)
		} else {
			delete(this.rules[key], rule_id)
		}
	}
	
	this.logger.Debug("Update rules: %v", this.rules)
	common.Rules = this.rules	
	
	this.logger.Info("Updater update %d rules over.", length)
}
	

func (this *Updater) Stop() {
	this.running = false
	this.logger.Info("Updater stop")
}





