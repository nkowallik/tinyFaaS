package util

type IpWrapper struct {
	Ip  string
	Cid string
}

type StatusMessage struct {
	CpuUsage        float64 `json:"cpuUsage"`
	RamUsage        float64 `json:"ramUsage"`
	Running         int     `json:"running"`
	InUse           int     `json:"inuse"`
	TooManyRequests int64   `json:"toomanyrequests"`
}
