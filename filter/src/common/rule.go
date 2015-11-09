package common

type Rule struct {
	hostname          string
	ruleId            int
	name              string
	treeId            int
	itemId            int
	compareType       string    // > >= < <=
	threshold         float64
	alarmGroup        string
	count             int 
	maxCount          int	
	abnormal_callback string
	recover_callback  string
}

func NewRule(host string, rule_id int, item_id int, cmp_type string, thresh float64) *Rule {
	r := &Rule{
		hostname: host,
		ruleId: rule_id,
		itemId: item_id,
		compareType: cmp_type,
		threshold: thresh,
	}
	return r
}


func (this *Rule) CheckStatus(val float64) int {
	var abnormal bool = false
	switch this.compareType {
		case ">":
			abnormal = val > this.threshold
		case ">=":
			abnormal = val >= this.threshold
		case "<":
			abnormal = val < this.threshold
		case "<=":
			abnormal = val <= this.threshold
	}
	
	if abnormal {
		return STATUS_ABNORMAL
	} else {
		return STATUS_NORMAL
	}
}
