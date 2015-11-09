package common

import (
	"fmt"
)

const (
	STATUS_NORMAL   int = 1
	STATUS_ABNORMAL int = 0
) 

type Packet struct {
	Hostname string
	RuleId   int
	ItemId   int
	Time     int
	Value    float64 
	Status   int 
}

func NewPacket(host string, item_id int, val float64) *Packet{
	
	return &Packet{
		Hostname: host,
		ItemId: item_id,
		Value: val,
	}
}

//encode for alarm
func (ap *Packet)ConvertToBytesForBackend(ruleId int, status int) ([]byte, error) {
    if len(ap.Hostname) == 0 {
	return nil, fmt.Errorf("Hostname empty")
    }

    body_str := fmt.Sprintf("%s\x01#%d\x01#%d\x01#%f\x01#%d", ap.Hostname, ruleId, ap.ItemId, ap.Value, status)
    body := []byte(body_str)
    body_len := len(body)
    if body_len == 0 || body_len > 102400 {
	return nil, fmt.Errorf("body length must between 0 ~ 100k")
    }

    head_str := fmt.Sprintf("%010d", body_len)
    head := []byte(head_str)
    if len(head) != 10 {
	return nil, fmt.Errorf("head length must equals to 10")
    }

    body_len += 10
    bytes := make([]byte, body_len) 
    i := 0
    for i < 10 {
	bytes[i] = head[i] 
	i += 1
    }
    j := 0
    for i < body_len {
	bytes[i] = body[j]
	i += 1
	j += 1
    }
    return bytes, nil
}
