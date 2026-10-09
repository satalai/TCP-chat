package main

//压测程序：起一批模拟客户端压TCP聊天服务端，最后打一张报表
//每个客户端：按-ramp的时刻表拨号 → 报名字 → 一直读广播（计数+抽样量延迟）→ 按-hold保持 → 按-message-every发消息
//时间戳用nowNano（纳秒精度的单调时钟），实现在clock_windows.go/clock_other.go

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 错误样本限量：匿名计数说不出失败原因，样本能；打太多会刷屏，限个量
const errorSampleLimit = 10

var errorSamples atomic.Int32

func reportError(stage string, err error) {
	//if初始化加条件判断的简便写法
	if n := errorSamples.Add(1); n <= errorSampleLimit {
		fmt.Printf("error sample %d/%d [%s]: %v\n", n, errorSampleLimit, stage, err)
	}
}

// updateMax用CAS循环把dst抬到v：多个goroutine同时更新时不会互相覆盖
func updateMax(dst *atomic.Int64, v int64) {
	for {
		old := dst.Load()
		if v <= old || dst.CompareAndSwap(old, v) {
			return
		}
	}
}

// ===== 延迟直方图 =====
// 延迟样本太多，几千个连接全存原始值内存扛不住，所以按格子数：
// 0~10ms每格100us，10~100ms每格1ms，100ms~1s每格10ms，1s以上统一进最后一格
// 每个客户端一份，写入的只有自己那个读取goroutine，不用加锁，收尾时再合并
const (
	histBuckets        = 281
	histOverflowBucket = 280

	//每收到这么多条数据消息抽1条量延迟，别让压测程序自己把CPU吃光
	latencySampleEvery = 10
)

type latencyHist [histBuckets]uint64

// 把一个耗时塞进对应的格子
func histIndex(ns int64) int {
	switch {
	case ns < 10*int64(time.Millisecond):
		return int(ns / int64(100*time.Microsecond)) //0~10ms，每格100us
	case ns < 100*int64(time.Millisecond):
		return 100 + int(ns/int64(time.Millisecond)) - 10 //10~100ms，每格1ms
	case ns < int64(time.Second):
		return 190 + int(ns/int64(10*time.Millisecond)) - 10 //100ms~1s，每格10ms
	default:
		return histOverflowBucket //1s以上
	}
}

// 取格子代表的耗时，报百分位用，取格子中点
func histMid(idx int) time.Duration {
	switch {
	case idx < 100:
		return time.Duration(idx)*100*time.Microsecond + 50*time.Microsecond
	case idx < 190:
		return time.Duration(10+idx-100)*time.Millisecond + 500*time.Microsecond
	case idx < histOverflowBucket:
		return time.Duration(100+(idx-190)*10)*time.Millisecond + 5*time.Millisecond
	default:
		return time.Second
	}
}

// 从直方图里取第p分位（p用0.5、0.95这种）
func percentileOf(hist []uint64, total uint64, p float64) (time.Duration, bool) {
	if total == 0 {
		return 0, false
	}
	rank := uint64(float64(total)*p) + 1 //要取第几名
	if rank > total {
		rank = total
	}
	var cum uint64
	for i := 0; i < histBuckets; i++ {
		cum += hist[i]
		if cum >= rank {
			return histMid(i), true
		}
	}
	return time.Second, true
}

// 从收到的一行里找",t="后面的发送时间戳，系统行（"xx: 上线了"这种）没有，返回0
func parseSendTime(line string) int64 {
	i := strings.LastIndex(line, ",t=")
	if i < 0 {
		return 0
	}
	rest := line[i+3:]
	j := strings.IndexByte(rest, ']')
	if j < 0 {
		return 0
	}
	ns, err := strconv.ParseInt(rest[:j], 10, 64)
	if err != nil {
		return 0
	}
	return ns
}

// 延迟打印成ms保留一位小数，没样本就打n/a
func fmtLatency(d time.Duration, ok bool) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}

func main() {
	//创建flag参数方便一份代码能在终端复用
	addr := flag.String("addr", "127.0.0.1:8080", "TCP server address")
	users := flag.Int("users", 100, "number of simulated users")
	hold := flag.Duration("hold", 60*time.Second, "connection hold duration")
	ramp := flag.Duration("ramp", 10*time.Second, "spread the dials evenly over this duration;0 dials all at once")
	reportEvery := flag.Duration("report-every", 5*time.Second, "print a progress line every interval;0 disables")
	//每多少人一组：2就相当于私聊，等于users就是一个大群，0=不分组（都待在大厅）
	group := flag.Int("group", 0, "how many users per group;2=private chat,0=no grouping")
	//发送消息的频率
	messageEvery := flag.Duration("message-every", 0, "interval between public message per user;0 disables sending")
	message := flag.String("message", "hello", "public message body")
	flag.Parse()

	if *group < 0 || *group == 1 {
		fmt.Println("group 要么是 0（不分组），要么 >=2（2 就是私聊）")
		return
	}

	//连接相关的计数
	var connected atomic.Int64 //累计连上过的连接数
	var alive atomic.Int64     //当前还活着（没退出）的连接数
	var dialFailed atomic.Int64
	var nameFailed atomic.Int64

	//消息相关的计数
	var sent atomic.Int64
	var sendFailed atomic.Int64   //写失败（非超时）
	var writeTimeout atomic.Int64 //写超时被掐断
	var recvFailed atomic.Int64   //接收失败（不是自己主动关的、读断了）
	var latencyMax atomic.Int64   //延迟样本里的最大值（纳秒）

	//每个客户端一份接收计数和延迟直方图，各写各的互不打扰，最后统一汇总
	receivedPer := make([]int64, *users)
	hists := make([]latencyHist, *users)

	//收尾时等所有读取goroutine都退干净了再汇总，不然会和它们抢着写这些数据
	var readersWG sync.WaitGroup

	//start给所有goroutine一个统一的起跑时刻：close之后才开始按-ramp的时刻表拨号
	start := make(chan struct{})
	begin := time.Now() //基准时间：第i个用户的拨号时刻 = begin + i*ramp/users

	//wg是waitGroup等待组，等每个用户的goroutine都退出
	var wg sync.WaitGroup
	wg.Add(*users)

	for i := 0; i < *users; i++ {
		id := i
		go func() {
			defer wg.Done()
			<-start

			//-ramp>0时按顺序分批拨号，连接逐步上涨；-ramp=0时全部同一瞬间开始
			if *ramp > 0 {
				when := begin.Add(time.Duration(int64(id) * int64(*ramp) / int64(*users)))
				if delay := time.Until(when); delay > 0 {
					time.Sleep(delay)
				}
			}

			//重要：开始拨号，超时算拨号失败
			conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
			if err != nil {
				dialFailed.Add(1)
				reportError("dial", err)
				return
			}
			//关连接前先把closing立起来，读取goroutine靠它区分"自己收工"和"真读坏了"
			var closing atomic.Bool
			defer func() {
				closing.Store(true)
				conn.Close()
			}()

			//第一行必须是用户名，服务端靠它把这条连接登记成一个用户
			if _, err := fmt.Fprintf(conn, "load-user-%05d\n", id); err != nil {
				nameFailed.Add(1)
				reportError("username", err)
				return
			}
			//分了组就再报一次：按连接顺序每group个人一组，组名g-0、g-1...
			if *group > 0 {
				if _, err := fmt.Fprintf(conn, "join|g-%d\n", id / *group); err != nil {
					nameFailed.Add(1)
					reportError("join", err)
					return
				}
			}

			connected.Add(1)
			alive.Add(1)
			defer alive.Add(-1) //任何退出路径都会把alive减回去

			//必须持续读取服务端广播，否则服务端可能因socket缓冲堆积而阻塞
			//这里顺便把数据消息数下来、抽一部分量端到端延迟；系统行（末尾不是]）直接跳过
			readersWG.Add(1)
			go func() {
				defer readersWG.Done()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						//不是我们主动关的才算接收失败
						if !closing.Load() {
							recvFailed.Add(1)
						}
						return
					}
					//数据消息的行尾是"[user=...,sequence=...,t=...]"，系统行不是
					if len(line) < 2 || line[len(line)-2] != ']' {
						continue
					}
					receivedPer[id]++
					//每latencySampleEvery条抽1条量延迟，样本够用还省CPU
					if receivedPer[id]%latencySampleEvery == 0 {
						if sendAt := parseSendTime(line); sendAt > 0 {
							if d := nowNano() - sendAt; d >= 0 {
								hists[id][histIndex(d)]++
								//比一比，把更大的延迟更新进latencyMax
								updateMax(&latencyMax, d)
							}
						}
					}
				}
			}()

			//设置stop计时器，时间是hold的数值
			stop := time.NewTimer(*hold)
			defer stop.Stop()

			//不传-message-every或其值为0时，只保持连接
			if *messageEvery <= 0 {
				<-stop.C
				return
			}

			//设置ticker触发器，每messageEvery发一次
			ticker := time.NewTicker(*messageEvery)
			defer ticker.Stop()
			var sequence int64 //第几条消息，从1开始
			for {
				select {
				case <-stop.C:
					return
				case <-ticker.C:
					sequence++

					//给单次发送设置超时，避免服务器卡住时压测用户永久阻塞
					_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))

					//把发送时间塞进消息里，收到的人用它算这条消息走了多久
					sendAt := nowNano()
					_, err := fmt.Fprintf(
						conn,
						"%s [user=%05d,sequence=%d,t=%d]\n",
						*message,
						id,
						sequence,
						sendAt,
					)

					//清除超时设置，避免影响后续操作
					_ = conn.SetWriteDeadline(time.Time{})

					//这是上面Fprintf的错误处理
					if err != nil {
						if errors.Is(err, os.ErrDeadlineExceeded) {
							writeTimeout.Add(1)
						} else {
							sendFailed.Add(1)
						}
						reportError("send", err)
						return
					}

					sent.Add(1)
				}
			}
		}()
	}

	//此时所有goroutine都创建好了，close(start)让它们按-ramp的时刻表开始拨号
	close(start)

	//运行中打进度行；结束前停掉，免得和最终报表交叉输出
	stopReport := make(chan struct{})
	if *reportEvery > 0 {
		go func() {
			ticker := time.NewTicker(*reportEvery)
			defer ticker.Stop()
			for {
				select {
				case <-stopReport:
					return
				case <-ticker.C:
					fmt.Printf(
						"[%s] alive=%d, connected=%d, dial_failed=%d, sent=%d, send_failed=%d, write_timeout=%d\n",
						time.Since(begin).Round(time.Second), //四舍五入到整数秒
						alive.Load(),
						connected.Load(),
						dialFailed.Load(),
						sent.Load(),
						sendFailed.Load(),
						writeTimeout.Load(),
					)
				}
			}
		}()
	}

	wg.Wait()
	//等待wg为0后关闭进度汇报
	close(stopReport)
	//等所有读取goroutine也退干净，再汇总它们攒的数据
	readersWG.Wait()
	elapsed := time.Since(begin)

	//把每个客户端的接收计数和直方图合并成总数
	var merged [histBuckets]uint64
	var receivedTotal int64
	var latencySamples uint64
	for i := 0; i < *users; i++ {
		receivedTotal += receivedPer[i]
		for b := 0; b < histBuckets; b++ {
			merged[b] += hists[i][b]
		}
	}
	for b := range merged {
		latencySamples += merged[b]
	}

	//下面按固定格式打报表
	fmt.Println("========== TCP Chat Load Test Result ==========")
	fmt.Println()

	fmt.Println("Connections")
	fmt.Printf("  %-15s%d\n", "Target:", *users)
	success := connected.Load()
	failed := dialFailed.Load() + nameFailed.Load()
	fmt.Printf("  %-15s%d\n", "Success:", success)
	fmt.Printf("  %-15s%d\n", "Failed:", failed)
	successRate := 0.0
	if *users > 0 {
		successRate = float64(success) / float64(*users) * 100
	}
	fmt.Printf("  %-15s%.2f%%\n", "Success Rate:", successRate)
	fmt.Println()

	//Received是"所有客户端收到的份数"：服务端广播是给其他每个人各发一份，
	//所以正常时它约等于 Sent×（当时在线人数-1），差得多就是服务端漏发了
	sentTotal := sent.Load()
	fmt.Println("Messages")
	fmt.Printf("  %-15s%d\n", "Sent:", sentTotal)
	fmt.Printf("  %-15s%d\n", "Received:", receivedTotal)
	mps := 0.0
	if elapsed > 0 {
		mps = float64(sentTotal) / elapsed.Seconds()
	}
	fmt.Printf("  %-15s%.0f msg/s\n", "Throughput:", mps)
	fmt.Println()

	//延迟是"消息里的t=到本地收到"这段时长，每latencySampleEvery条抽1条，报的是抽样分布
	fmt.Println("Latency")
	p50, ok50 := percentileOf(merged[:], latencySamples, 0.50)
	p95, ok95 := percentileOf(merged[:], latencySamples, 0.95)
	p99, ok99 := percentileOf(merged[:], latencySamples, 0.99)
	fmt.Printf("  %-15s%s\n", "P50:", fmtLatency(p50, ok50))
	fmt.Printf("  %-15s%s\n", "P95:", fmtLatency(p95, ok95))
	fmt.Printf("  %-15s%s\n", "P99:", fmtLatency(p99, ok99))
	fmt.Printf("  %-15s%s\n", "Max:", fmtLatency(time.Duration(latencyMax.Load()), latencySamples > 0))
	fmt.Println()

	//发送失败是写失败+写超时加一起，接收失败是"不是自己主动关的、读断了"
	sendFailTotal := sendFailed.Load() + writeTimeout.Load()
	recvFail := recvFailed.Load()
	fmt.Println("Errors")
	fmt.Printf("  %-15s%d\n", "Send Failed:", sendFailTotal)
	fmt.Printf("  %-15s%d\n", "Receive Failed:", recvFail)
	errRate := 0.0
	if sentTotal > 0 {
		errRate = float64(sendFailTotal+recvFail) / float64(sentTotal) * 100
	}
	fmt.Printf("  %-15s%.3f%%\n", "Error Rate:", errRate)
	fmt.Println()

	fmt.Println(strings.Repeat("=", 47))
}
