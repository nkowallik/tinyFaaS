package util

import "time"

type IpWrapper struct {
	Ip  string
	Cid string
}

type StatusMessage struct {
	CpuUsage        float64 `json:"cpuUsage"`
	RamUsage        float64 `json:"ramUsage"`
	Running         int     `json:"running"`
	InUse           int     `json:"inuse"`
	TooManyRequests int     `json:"toomanyrequests"`
}

type ClusterNodeMessage struct {
	Address         string    `json:"address"`
	CpuUsage        float64   `json:"cpuusage"`
	RamUsage        float64   `json:"ramusage"`
	CpuHistory      []float64 `json:"cpuhistory"`
	RamHistory      []float64 `json:"ramhistory"`
	Running         int       `json:"running"`
	InUse           int       `json:"used"`
	Requests        int       `json:"requests"`
	Up              bool      `json:"up"`
	LastUsed        time.Time `json:"lastused"`
	IsMaster        bool      `json:"ismaster"`
	TooManyRequests int       `json:"toomanyrequests"`
}
