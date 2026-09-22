package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	//创建flag参数方便一份代码能在终端复用
	addr := flag.String("addr", "127.0.0.1:8080", "TCP server address")
	users := flag.Int("users", 100, "number of simulated users")
	hold := flag.Duration("hold", 60*time.Second, "connection hold duration")
	//发送消息的频率
	messageEvery := flag.Duration(
		"message-every",
		0,
		"interval between public message per user;0 disables sending",
	)
	message := flag.String("message","hello","public message body")
	flag.Parse()

	var connected atomic.Int64
	var failed atomic.Int64
	var sent atomic.Int64
	var sendFailed atomic.Int64

	start := make(chan struct{})
	
	var wg sync.WaitGroup
	wg.Add(*users)

	for i := 0; i < *users; i++ {
		id := i
		go func() {
			defer wg.Done()
			<-start

			conn, err := net.DialTimeout("tcp", *addr, 5*time.Second)
			if err != nil {
				failed.Add(1)
				return
			}
			defer conn.Close()

			if _, err := fmt.Fprintf(conn, "load-user-%05d\n", id); err != nil {
				failed.Add(1)
				return
			}

			connected.Add(1)

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
					_ = conn.SetWriteDeadline(time.Now().Add(3* time.Second))

					_ ,err := fmt.Fprintf(
						conn,
						"%s [user=%05d,sequence=%d]\n",
						*message,
						id,
						sequence,
					)

					// 清除超时设置，避免影响后续操作
					_ = conn.SetWriteDeadline(time.Time{})

					if err != nil{
						sendFailed.Add(1)
						return
					}

					sent.Add(1)
				}
			}
		}()
	}

	begin := time.Now()
	close(start)
	wg.Wait()

	fmt.Printf(
		"finished in %s, connected=%d,connect_failed=%d,sent=%d,send_failed=%d\n",
		time.Since(begin).Round(time.Millisecond),
		connected.Load(),
		failed.Load(),
		sent.Load(),
		sendFailed.Load(),
	)
}
