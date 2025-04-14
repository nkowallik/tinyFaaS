package util

type IpWrapper struct {
	Ip  string
	Cid string
}

type StatusMessage struct {
	CpuUsage float64          `json:"cpuUsage"`
	RamUsage float64          `json:"ramUsage"`
	Running  map[string]int16 `json:"running"`
}
