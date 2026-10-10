package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
)

// metricsSampleEvery 采样间隔，1秒一次
// 为什么要后台采样而不是请求来了现算：CPU是"两次读数差分"算的，现算第一次没基线；
// 1秒比5秒更容易抓到广播瞬间的尖峰，所有"每秒"类指标都按实际流逝时间折算
const metricsSampleEvery = 1 * time.Second

var (
	//进程启动时间，用来算uptime
	metricsStart = time.Now()

	//保护metricsLatest
	metricsMu sync.RWMutex
	//最近一次采样，HTTP请求直接读它
	metricsLatest *metricsSnapshot
)

// cpuStats 进程和整机的CPU占用
// 进程口径：100%=占满一个核，多核机器上超过100正常（250%=2.5个核）
// 整机口径：所有核合计的0-100，100%=全部核都在忙
type cpuStats struct {
	ProcessPercent float64 `json:"process_percent"`
	SystemPercent  float64 `json:"system_percent"`
}

// memStats 内存占用（单位都是MiB，按1024*1024换算）
// RSS对应任务管理器里的"工作集"；Go堆/栈是Go运行时自己记的账
// RSS ≈ 堆Sys + 栈Sys + 运行时元数据 + 二进制映射，看栈能解释"RSS为什么比堆大那么多"
type memStats struct {
	RSSMb           float64 `json:"rss_mb"`
	GoHeapAllocMb   float64 `json:"go_heap_alloc_mb"`   //堆上存活+未回收对象
	GoHeapInuseMb   float64 `json:"go_heap_inuse_mb"`   //堆span正在使用的部分
	GoHeapSysMb     float64 `json:"go_heap_sys_mb"`     //堆向系统申请的总量
	GoStackInuseMb  float64 `json:"go_stack_inuse_mb"`  //所有goroutine栈，连接多时这项会涨
	GoNumGC         uint32  `json:"go_num_gc"`          //累计GC次数
}

// allocStats 分配与GC的"速率"，全是相邻两次采样的增量，不是累计值
// 这是对比广播优化前后最直接的指标
type allocStats struct {
	MallocsPerSec  float64 `json:"mallocs_per_sec"`   //每秒分配多少次（对象个数）
	AllocMbPerSec  float64 `json:"alloc_mb_per_sec"`  //每秒分配多少MiB（累计分配量增量，已释放的也算）
	GCPerSec       float64 `json:"gc_per_sec"`        //每秒GC几次
	GCPauseMsInWin float64 `json:"gc_pause_ms_in_window"` //本采样窗口内GC停顿累计毫秒
	MallocsTotal   uint64  `json:"mallocs_total"`     //累计分配次数
}

// metricsSnapshot 一次采样的全部指标
type metricsSnapshot struct {
	Time          string     `json:"time"`
	UptimeSeconds float64    `json:"uptime_seconds"`
	OnlineUsers   int        `json:"online_users"`
	Goroutines    int        `json:"goroutines"`
	Threads       int        `json:"threads"` //操作系统线程数，0表示读取失败
	CPU           cpuStats   `json:"cpu"`
	Memory        memStats   `json:"memory"`
	Alloc         allocStats `json:"alloc"`
}

// round1 保留1位小数，免得JSON里冒出一串没意义的精度
func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func toMb(b uint64) float64 {
	return float64(b) / 1024 / 1024
}

// startMetrics 起两样东西：后台采样goroutine + 只读快照的HTTP接口
// 接口只绑本机127.0.0.1:8081，和pprof一个规矩；8081起不来也不影响聊天主服务
func startMetrics(um *userManager) {
	//为当前进程创建一个进程对象,后面才能用它查 CPU、内存等信息
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		fmt.Println("metrics 初始化失败:", err)
		return
	}

	//先各踩一脚基线：CPU都是差分算的，没有第一次读数第二个数出不来
	primeTimes, err := proc.Times()
	if err != nil {
		fmt.Println("metrics 初始化失败:", err)
		return
	}
	cpu.Percent(0, false) //系统CPU也调一次，让gopsutil记住"上次读数"

	go func() {
		ticker := time.NewTicker(metricsSampleEvery)
		defer ticker.Stop()

		prevProcCPU := primeTimes.User + primeTimes.System //进程累计CPU秒数，差分用
		lastSampleAt := time.Now()

		//分配/GC的累计值基线：之后每次用"本次-上次"得到窗口内增量
		var prevMem runtime.MemStats
		runtime.ReadMemStats(&prevMem)

		for range ticker.C {
			now := time.Now()
			elapsed := now.Sub(lastSampleAt).Seconds()
			lastSampleAt = now

			var cpuStat cpuStats

			//进程CPU = 累计CPU秒数增量 ÷ 实际流逝时间；100%=一个核跑满
			//除实际流逝时间而不是固定间隔：ticker晚触发一点数也不会算歪
			if t, err := proc.Times(); err == nil {
				cur := t.User + t.System
				if elapsed > 0 {
					cpuStat.ProcessPercent = round1((cur - prevProcCPU) / elapsed * 100)
				}
				prevProcCPU = cur
			}

			//整机CPU：传0表示"距上次调用以来"，prime那次之后每个数都是采样窗口的均值
			if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
				cpuStat.SystemPercent = round1(percents[0])
			}

			//内存看两条线：RSS是进程总共吃多少（含Go之外的），Go内部再细分堆和栈
			var memStat memStats
			if mi, err := proc.MemoryInfo(); err == nil && mi != nil {
				memStat.RSSMb = round1(toMb(mi.RSS))
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memStat.GoHeapAllocMb = round1(toMb(m.HeapAlloc))
			memStat.GoHeapInuseMb = round1(toMb(m.HeapInuse))
			memStat.GoHeapSysMb = round1(toMb(m.HeapSys))
			memStat.GoStackInuseMb = round1(toMb(m.StackInuse))
			memStat.GoNumGC = m.NumGC

			//分配与GC速率：累计值做差，再除以实际流逝时间
			var allocStat allocStats
			allocStat.MallocsTotal = m.Mallocs
			if elapsed > 0 {
				allocStat.MallocsPerSec = math.Round(float64(m.Mallocs-prevMem.Mallocs) / elapsed)
				allocStat.AllocMbPerSec = round1(toMb(m.TotalAlloc-prevMem.TotalAlloc) / elapsed)
				allocStat.GCPerSec = round1(float64(m.NumGC-prevMem.NumGC) / elapsed)
			}
			allocStat.GCPauseMsInWin = math.Round(float64(m.PauseTotalNs-prevMem.PauseTotalNs)/1e6*100) / 100
			prevMem = m

			//线程数：Windows下可用；失败就留0，不影响其他指标
			threads := 0
			if n, err := proc.NumThreads(); err == nil {
				threads = int(n)
			}

			um.RLock()
			online := len(um.users) //在线人数就在手边，顺手带上
			um.RUnlock()

			snap := &metricsSnapshot{
				Time:          now.Format(time.RFC3339),
				UptimeSeconds: round1(now.Sub(metricsStart).Seconds()),
				OnlineUsers:   online,
				Goroutines:    runtime.NumGoroutine(), //每条连接1个goroutine，2000连接就2000+
				Threads:       threads,
				CPU:           cpuStat,
				Memory:        memStat,
				Alloc:         allocStat,
			}

			metricsMu.Lock()
			metricsLatest = snap
			metricsMu.Unlock()
		}
	}()

	//HTTP接口单独起一个，用独立的mux：6060的pprof走DefaultServeMux，互不串门
	go func() {
		addr := "127.0.0.1:8081"
		fmt.Println("metrics listening at http://" + addr + "/metrics")
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", handleMetrics)
		err := http.ListenAndServe(addr, mux)
		if err != nil {
			fmt.Println("metrics server stopped:", err)
		}
	}()
}

// handleMetrics 只回最近一次快照，不做任何现场计算
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	metricsMu.RLock()
	snap := metricsLatest
	metricsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if snap == nil {
		//刚启动头1秒还没采到第一个数，如实告诉对方
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, `{"error":"metrics warming up, first sample in a few seconds"}`)
		return
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(append(data, '\n'))
}
