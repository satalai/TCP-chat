package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// 错误样本限量：匿名计数说不出失败原因，样本能；打太多会刷屏，限个量。
const errorSampleLimit = 10

var errorSamples atomic.Int32

func reportError(stage string, err error) {
	if n := errorSamples.Add(1); n <= errorSampleLimit {
		fmt.Printf("error sample %d/%d [%s]: %v\n", n, errorSampleLimit, stage, err)
	}
}

func main() {
	//创建flag参数方便一份代码能在终端复用
	addr := flag.String("addr", "127.0.0.1:8080", "TCP server address")
	users := flag.Int("users", 100, "number of simulated users")
	hold := flag.Duration("hold", 60*time.Second, "connection hold duration")
	// 连接分批建立：把 -users 个连接的拨号时刻均匀铺在 ramp 时长内，连接逐步上涨；
	// 0 = 全部同一瞬间开始（旧行为）。
	ramp := flag.Duration("ramp", 10*time.Second, "spread the dials evenly over this duration;0 dials all at once")
	// 运行中每隔一段时间打一行进度（看 alive 的涨落）；0 = 不打印。
	reportEvery := flag.Duration("report-every", 5*time.Second, "print a progress line every interval;0 disables")
	//发送消息的频率
	messageEvery := flag.Duration(
		"message-every",
		0,
		"interval between public message per user;0 disables sending",
	)
	message := flag.String("message", "hello", "public message body")
	flag.Parse()

	var connected atomic.Int64    // 累计连上过的连接数
	var alive atomic.Int64        // 当前还活着（没退出）的连接数
	var dialFailed atomic.Int64   // 拨号失败次数
	var nameFailed atomic.Int64   // 连上后发用户名失败次数
	var sent atomic.Int64         // 成功发出的消息数
	var sendFailed atomic.Int64   // 发送失败次数（非超时类）
	var writeTimeout atomic.Int64 // 发送超时次数（写超时被掐断）

	// start 给所有 goroutine 一个统一的起跑时刻：close 之后才开始按 -ramp 的
	// 时刻表拨号（-ramp=0 时就是全部同一瞬间）。
	start := make(chan struct{})

	// 基准时间：第 i 个用户的拨号时刻 = begin + i*ramp/users。
	begin := time.Now()

	var wg sync.WaitGroup
	wg.Add(*users)

	for i := 0; i < *users; i++ {
		id := i
		go func() {
			defer wg.Done()
			<-start

			// -ramp>0 时按顺序分批拨号，连接逐步上涨；-ramp=0 时保持旧行为，全部同一瞬间开始。
			if *ramp > 0 {
				when := begin.Add(time.Duration(int64(id) * int64(*ramp) / int64(*users)))
				if delay := time.Until(when); delay > 0 {
					time.Sleep(delay)
				}
			}

			conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
			if err != nil {
				dialFailed.Add(1)
				reportError("dial", err)
				return
			}
			defer conn.Close()

			if _, err := fmt.Fprintf(conn, "load-user-%05d\n", id); err != nil {
				nameFailed.Add(1)
				reportError("username", err)
				return
			}

			connected.Add(1)
			alive.Add(1)
			defer alive.Add(-1) // 任何退出路径都会把 alive 减回去

			// 必须持续读取服务端广播，否则服务端可能因 socket 缓冲堆积而阻塞。
			done := make(chan struct{})
			go func() {
				defer close(done)
				reader := bufio.NewReader(conn)
				for {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
				}
			}()

			stop := time.NewTimer(*hold)
			defer stop.Stop()

			// 不传 -message-every 或其值为0时，只保持连接
			if *messageEvery <= 0 {
				<-stop.C
				return
			}

			ticker := time.NewTicker(*messageEvery)
			defer ticker.Stop()

			var sequence int64
			for {
				select {
				case <-stop.C:
					return
				case <-ticker.C:
					sequence++

					// 给单次发送设置超时，避免服务器卡住时压测用户永久阻塞
					_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))

					_, err := fmt.Fprintf(
						conn,
						"%s [user=%05d,sequence=%d]\n",
						*message,
						id,
						sequence,
					)

					// 清除超时设置，避免影响后续操作
					_ = conn.SetWriteDeadline(time.Time{})

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

	close(start)

	// 运行中打进度行；结束前停掉，免得和最终汇总交叉输出。
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
						time.Since(begin).Round(time.Second),
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
	close(stopReport)

	fmt.Printf(
		"finished in %s, users=%d, connected=%d, dial_failed=%d, name_failed=%d, sent=%d, send_failed=%d, write_timeout=%d\n",
		time.Since(begin).Round(time.Millisecond),
		*users,
		connected.Load(),
		dialFailed.Load(),
		nameFailed.Load(),
		sent.Load(),
		sendFailed.Load(),
		writeTimeout.Load(),
	)
}
