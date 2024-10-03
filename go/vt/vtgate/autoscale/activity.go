package autoscale

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

var (
	LastActiveTimestamp *stats.Gauge
	activityChan        chan struct{}
)

func RecordActivity() {
	LastActiveTimestamp.Set(time.Now().Unix())
	select {
	case activityChan <- struct{}{}:
	default:
	}
}

func WatchActivity() <-chan struct{} {
	return activityChan
}

func GetLastActiveTimestampsFromVTGates() []int64 {
	type DebugVars struct {
		LastActiveTimestamp int64 `json:"LastActiveTimestamp"`
	}

	rst := make([]int64, 0)

	serviceName := AutoScaleVTGateHeadlessServiceName

	ips, err := net.LookupIP(serviceName)
	if err != nil {
		log.Error("Failed to lookup IP addresses for service %s: %v", serviceName, err)
		return nil
	}

	for _, ip := range ips {
		url := fmt.Sprintf("http://%s:15001/debug/vars", ip)

		resp, err := http.Get(url)
		if err != nil {
			log.Errorf("Failed to send request to %s: %v", ip, err)
			continue
		}
		func() {
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Errorf("Failed to read response body from %s: %v", ip, err)
				return
			}

			var debugVars DebugVars
			if err := json.Unmarshal(body, &debugVars); err != nil {
				log.Errorf("Failed to unmarshal JSON from %s: %v", ip, err)
				return
			}

			rst = append(rst, debugVars.LastActiveTimestamp)
		}()
	}
	return rst
}
