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

// 压测参数，全部可以在命令行覆盖，例如：
//
//	go run ./cmd/loadtest -n 10 -interval 2s -duration 20s -v
var (
	serverAddr = flag.String("addr", "localhost:8080", "服务器地址")
	clientNum  = flag.Int("n", 100, "并发客户端数量")
	interval   = flag.Duration("interval", 2*time.Second, "每个客户端发送一句 hello 的间隔")
	duration   = flag.Duration("duration", 30*time.Second, "压测持续时长")
	verbose    = flag.Bool("v", false, "打印每个客户端收到的消息（量很大，建议只在小规模压测时开启）")

	// 三个网络超时。少了它们，任何一次网络异常都会让压测程序永久挂住：
	// 挂住的压测程序不会报错，只会安静地不动，比崩溃更难排查。
	dialTimeout  = flag.Duration("dial-timeout", 3*time.Second, "建立连接的超时")
	writeTimeout = flag.Duration("write-timeout", 5*time.Second, "单次发送的超时（对端不读就会撞上它）")
	idleTimeout  = flag.Duration("idle-timeout", 0, "多久收不到服务端任何消息就判定服务端卡住；0 表示自动取 5×interval")
	grace        = flag.Duration("grace", 5*time.Second, "总时长到了之后，再给客户端多少宽限期退出")
)

// counters 记录压测过程中的累计量。
// 多个客户端 goroutine 会并发累加，所以统一用 atomic 操作读写。
type counters struct {
	connected int64 // 连接成功并已发送用户名的客户端数
	sent      int64 // 成功发出的 hello 条数
	received  int64 // 收到的服务端消息条数（主要是别人 hello 的广播）

	// 下面这些按"失败原因"分开记，因为不同原因指向完全不同的问题。
	sendErrs      int64 // 写失败（非超时），通常是连接被对端重置
	recvErrs      int64 // 非正常断开（非超时）
	dialTimeouts  int64 // 拨号超时：连不上，或目标被静默丢包
	writeTimeouts int64 // 写超时：服务端不读了，发送窗口被写满
	idleTimeouts  int64 // 空闲读超时：服务端长时间一行都不推，基本是广播卡住
}

func main() {
	flag.Parse()

	// idleTimeout 传 0 表示"自动"：取发送间隔的 5 倍。
	// 依据是本项目的流量模型——每个客户端每 interval 发一次，
	// n>=2 时服务端会把每一句 hello 广播给其他所有人。
	// 所以连续 5 个周期一行都没收到，基本可以断定广播卡住了。
	if *idleTimeout == 0 {
		*idleTimeout = *interval * 5
	}

	// 只有 1 个客户端时，服务端不广播给任何人（它不回显给自己），
	// 收不到消息完全是正常的，空闲检测必然误报，直接关掉。
	if *clientNum < 2 {
		*idleTimeout = 0
	}

	idleDesc := "关闭"
	if *idleTimeout > 0 {
		idleDesc = idleTimeout.String()
	}

	fmt.Printf("压测开始：%d 个客户端，每 %v 发送一句 hello，持续 %v，目标 %s\n",
		*clientNum, *interval, *duration, *serverAddr)
	fmt.Printf("超时设置：拨号 %v，单次写 %v，空闲读 %s\n", *dialTimeout, *writeTimeout, idleDesc)

	// stop 在压测时间到时被关闭，所有客户端都靠它退出。
	stop := make(chan struct{})

	var (
		wg    sync.WaitGroup
		cnt   counters
		start = time.Now()
	)

	// 计时 goroutine：到点关闭 stop，相当于给整个压测设一个总闸。
	go func() {
		time.Sleep(*duration)
		close(stop)
	}()

	// 进度 goroutine：每 5 秒报一次当前累计量，方便一边压一边看。
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				fmt.Printf("[%v] 已发送 %d 条 hello，已收到 %d 条服务端消息\n",
					time.Since(start).Round(time.Second),
					atomic.LoadInt64(&cnt.sent),
					atomic.LoadInt64(&cnt.received))
			}
		}
	}()

	wg.Add(*clientNum)
	for i := 0; i < *clientNum; i++ {
		go runClient(i, stop, &cnt, &wg)
	}

	// 兜底看门狗：正常情况所有客户端都在 duration 内退出，
	// 但万一还有意料之外的阻塞，也保证压测工具一定会结束并打印结果，
	// 而不是永远挂在那里让人分不清是"服务端慢"还是"程序死了"。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(*duration + *grace):
		fmt.Println("警告：超过宽限期仍有客户端没退出，强制汇总（正常情况下读/写超时应能避免）")
	}

	<-progressDone // 等进度 goroutine 收尾，避免它和汇总输出交叉打印

	printSummary(&cnt, time.Since(start))
}

// runClient 是单个压测客户端的完整生命周期：
// 连接 -> 发用户名 -> 起读 goroutine -> 每 interval 发一句 hello -> 时间到退出。
func runClient(id int, stop <-chan struct{}, cnt *counters, wg *sync.WaitGroup) {
	defer wg.Done()

	// 用 Dialer 而不是 net.Dial，因为它能带上两个超时相关的能力：
	//   Timeout   —— 整个"建连"过程（DNS + TCP 握手）的总超时；
	//   KeepAlive —— 开启 TCP keepalive，用来发现对端主机已经消失。
	// 不设 Timeout 的话，遇到被防火墙静默丢包的目标，这次 Dial 会一直
	// 卡到操作系统的默认值（Windows 上约 21 秒），压测还没开始就失去意义。
	dialer := net.Dialer{
		Timeout:   *dialTimeout,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.Dial("tcp", *serverAddr)
	if err != nil {
		if isTimeout(err) {
			atomic.AddInt64(&cnt.dialTimeouts, 1)
			fmt.Printf("客户端 %d 连接超时（>%v）：%v\n", id, *dialTimeout, err)
		} else {
			fmt.Printf("客户端 %d 连接失败：%v\n", id, err)
		}
		return
	}
	defer conn.Close()

	// 第一步：先报上用户名。
	// 服务端要读完这一行才会把连接注册进在线列表，之后发的 hello 才会被广播。
	username := fmt.Sprintf("user_%d\n", id)
	if err := writeWithTimeout(conn, []byte(username), *writeTimeout); err != nil {
		recordWriteErr(cnt, err)
		fmt.Printf("客户端 %d 发送用户名失败：%v\n", id, err)
		return
	}
	atomic.AddInt64(&cnt.connected, 1)

	// 第二步：起一个 goroutine 一直读。
	// 这一步不能省：服务端会把别人的 hello 广播给所有连接，
	// 如果本地没人读，内核接收缓冲区很快被塞满，
	// 服务端的 Write 就会阻塞在那里，压测结果随之失真。
	go readLoop(id, conn, stop, cnt)

	// 第三步：每 interval 发一句 hello。
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// 写超时通常意味着服务端不读了：它可能正卡在"给别人广播"上，
			// 于是这条连接的接收缓冲被填满、TCP 发送窗口归零。
			// 没有这个超时，goroutine 会永久停在 Write 里，
			// 压测结束时 wg.Wait() 也就永远等不到它。
			if err := writeWithTimeout(conn, []byte("hello\n"), *writeTimeout); err != nil {
				recordWriteErr(cnt, err)
				return
			}
			atomic.AddInt64(&cnt.sent, 1)
		}
	}
}

// readLoop 持续读取服务端推来的消息并计数。
// verbose 打开时会逐条打印，用来肉眼确认 hello 是不是真的被别人收到了。
func readLoop(id int, conn net.Conn, stop <-chan struct{}, cnt *counters) {
	reader := bufio.NewReader(conn)

	for {
		// 每次读之前把"截止时刻"往后推，实现"空闲超时"：
		// 只要连续 idleTimeout 内一行都没读到，就认为服务端停止推送了。
		// 注意 deadline 是绝对时刻，每轮都必须重算，否则设一次就永久生效。
		if *idleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(*idleTimeout)); err != nil {
				atomic.AddInt64(&cnt.recvErrs, 1)
				return
			}
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			// 这里的判断顺序很重要，四种情况的原因完全不同，不能混为一谈：
			//   1. stop 已关闭     -> 压测正常收工，客户端主动 Close，必须最先判断；
			//   2. net.ErrClosed   -> 连接是我们自己关的（典型场景：写超时后
			//                         runClient 返回并 Close），不是对端的问题；
			//   3. 超时            -> 服务端长时间一行不推，判定它卡住了；
			//   4. 其余（如 io.EOF）-> 对端把连接断了，这才是真正需要警惕的异常。
			select {
			case <-stop:
				return
			default:
			}

			if errors.Is(err, net.ErrClosed) {
				return
			}

			if isTimeout(err) {
				atomic.AddInt64(&cnt.idleTimeouts, 1)
				return
			}
			atomic.AddInt64(&cnt.recvErrs, 1)
			return
		}

		atomic.AddInt64(&cnt.received, 1)

		if *verbose {
			// line 自带换行符，这里不用再补 \n
			fmt.Printf("[客户端 %d 收到] %s", id, line)
		}
	}
}

// writeWithTimeout 设置一个截止时刻后写一次数据。
//
// 关键点：SetWriteDeadline 收的是**绝对时间点**，不是"时长"，
// 所以每次写之前都要重新算一遍 now+timeout；
// 而且写超时之后这个 deadline 仍然是过期的，
// 后续任何写入都会立刻失败——所以超时后正确的做法是关掉这条连接。
func writeWithTimeout(conn net.Conn, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	_, err := conn.Write(data)
	return err
}

// recordWriteErr 按失败原因分别记账，方便在汇总里区分"超时"和"真错"。
func recordWriteErr(cnt *counters, err error) {
	if isTimeout(err) {
		atomic.AddInt64(&cnt.writeTimeouts, 1)
		return
	}
	atomic.AddInt64(&cnt.sendErrs, 1)
}

// isTimeout 判断一个网络错误是不是"超时"（deadline 到期）。
// Go 1.15 起推荐用 errors.Is(err, os.ErrDeadlineExceeded)；
// net.Error.Timeout() 是更早的写法，这里两种都认，兼容性更好。
func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// printSummary 输出压测汇总。
// 关键校验点：服务端每收到一句 hello，会广播给其余 (n-1) 个客户端，
// 因此「收到总数」应该接近「发送总数 × (n-1)」；
// 如果明显偏小，说明服务端广播有丢消息或者卡住了。
func printSummary(cnt *counters, elapsed time.Duration) {
	connected := atomic.LoadInt64(&cnt.connected)
	sent := atomic.LoadInt64(&cnt.sent)
	received := atomic.LoadInt64(&cnt.received)

	// 理论上每个客户端每 interval 发一句，共 duration 这么久。
	expectedSent := int64(0)
	if *interval > 0 {
		expectedSent = int64(*duration / *interval)
	}

	expectedRecv := int64(0)
	if connected > 1 {
		expectedRecv = sent * (connected - 1)
	}

	perClient := float64(0)
	if connected > 0 {
		perClient = float64(received) / float64(connected)
	}

	fmt.Println()
	fmt.Println("========== 压测结果 ==========")
	fmt.Printf("耗时            : %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("连接成功        : %d / %d\n", connected, *clientNum)
	fmt.Printf("发送 hello      : %d 条（单人理论值约 %d 条）\n", sent, expectedSent)
	fmt.Printf("收到服务端消息  : %d 条（hello 广播理论值约 %d 条，另有上下线通知）\n",
		received, expectedRecv)
	fmt.Printf("平均每个客户端  : 收到 %.1f 条\n", perClient)
	fmt.Printf("超时            : 拨号 %d 次，写 %d 次，空闲读 %d 次\n",
		atomic.LoadInt64(&cnt.dialTimeouts),
		atomic.LoadInt64(&cnt.writeTimeouts),
		atomic.LoadInt64(&cnt.idleTimeouts))
	fmt.Printf("其他错误        : 发送失败 %d 次，异常断开 %d 次\n",
		atomic.LoadInt64(&cnt.sendErrs),
		atomic.LoadInt64(&cnt.recvErrs))
	fmt.Println("==============================")
}
