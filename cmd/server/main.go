package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

// verbose 控制每个连接的日志（接入、断开、读写失败）
// 默认关：几千个连接压测的时候这些日志会把控制台刷爆，每条Println都是实打实的I/O
var verbose bool

// outQueueSize 是每个连接的发送队列长度（条数）。
// 队列满 = 这个客户端消费得比我们生产得慢，直接踢掉，不能让它拖累别人。
const outQueueSize = 1024

// maxBatch 是 writeLoop 一次 writev 最多合并的消息条数。
const maxBatch = 128

// kickedCount 统计被"队列满"踢掉的客户端数量，压测时可以接进 metrics 里看。
var kickedCount atomic.Int64

type user struct {
	conn  net.Conn
	name  string //受 userManager 的锁保护
	group string //所在组，空串=大厅；受 userManager 的锁保护

	out       chan []byte   //发送队列：所有要发给这个客户端的数据都先进这里
	done      chan struct{} //连接清理时关闭，通知写 goroutine 退出
	closeOnce sync.Once     //优雅关闭，sync.Once 保证只关一次 done，避免写 goroutine 卡死
	kickOnce  sync.Once     //踢人，sync.Once 保证只踢一次，避免写 goroutine 卡死
}

// newUser 创建一个新的 user 实例，conn 是这个用户的连接，name 是用户名。
func newUser(conn net.Conn, name string) *user {
	return &user{
		conn: conn,
		name: name,
		out:  make(chan []byte, outQueueSize),
		done: make(chan struct{}),
	}
}

// writeLoop 是这个连接唯一会调用 conn.Write 的地方。
// 慢客户端只会卡住它自己的 writeLoop，不会卡住广播方，更不会卡住锁。
func (u *user) writeLoop() {
	for {
		select {
		case data := <-u.out:
			// 队列里攒了多少就一次性取出多少，合成一次 writev 系统调用，
			// 比一条消息一次 Write 的吞吐高得多，队列也就不容易被冲满。
			bufs := net.Buffers{data} //使用net.Buffers可以一次性写入多个字节切片，用到后面的WriteTo方法调用writev，减少系统调用次数，提高性能
		drain: //drain 标签让我们可以 break 这个 select，继续往下 drain 队列
			for len(bufs) < maxBatch {
				select {
				case more := <-u.out:
					bufs = append(bufs, more)
				default:
					break drain
				}
			} //drain标签的范围到此结束，这样不用凑到maxBatch就能直接进到下面的发送逻辑
			if _, err := bufs.WriteTo(u.conn); err != nil {
				if verbose {
					fmt.Println("发送消息失败:", err)
				}
				u.conn.Close() //让读循环也报错退出，走统一的清理流程
				return
			}
		case <-u.done:
			return
		}
	}
}

// send 把数据放进发送队列u.out里，永远不阻塞。队列满就踢人。
// 因为不阻塞，所以可以安全地在持有 um 的锁时调用。
func (u *user) send(data []byte) {
	select {
	case u.out <- data:
	default:
		u.kick()
	}
}

// kick 强制断开一个跟不上的客户端。SetLinger(0) 让 Close 直接发 RST、丢掉内核里没发完的数据，
// 否则 Close 会老老实实把积压的数据发完再发 FIN，而这个客户端恰恰就是读不动的。
// 读循环随后会收到错误，走统一的清理流程。
func (u *user) kick() {
	u.kickOnce.Do(func() { //once结构+Do方法，保证在多个goroutine操作下只会执行一次踢人
		if verbose {
			fmt.Println("发送队列已满，踢掉:", u.conn.RemoteAddr())
		}
		kickedCount.Add(1)
		if tc, ok := u.conn.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		u.conn.Close()
	})
}

// 这也是once数据类型，配合Do函数保证只执行一次Writeloop的关闭操作
func (u *user) shutdown() {
	u.closeOnce.Do(func() { close(u.done) })
}

// userManager 里 users 保持叫 users：startMetrics 如果只是 len(um.users) 或者 range 它，
// 切片换成 map 之后代码不用改。
type userManager struct { //维护三组索引，保证每次查询、删除用户都是O（1），空间换时间
	users  map[*user]struct{}            //所有在线用户
	byName map[string]*user              //名字 -> 用户，私聊/重名检查 O(1)
	groups map[string]map[*user]struct{} //组名 -> 成员，广播只遍历同组的人（"" = 大厅）
	sync.RWMutex
}

// 创建新的userManager
func newUserManager() *userManager {
	return &userManager{
		users:  make(map[*user]struct{}),
		byName: make(map[string]*user),
		groups: make(map[string]map[*user]struct{}),
	}
}

// 调用方必须已经持有写锁
// 把用户加入组的一个函数，若没有组就创建一个新的组，并把用户加入该组
func (um *userManager) addToGroupLocked(u *user) {
	//set 是组名对应的成员集合，map[*user]struct{} 只是为了 O(1) 查找和删除
	set := um.groups[u.group]
	if set == nil {
		set = make(map[*user]struct{})
		um.groups[u.group] = set
	}
	set[u] = struct{}{}
}

// 调用方必须已经持有写锁
// 将用户从组内删除
func (um *userManager) removeFromGroupLocked(u *user) {
	//set长这样：[*user]->struct{}
	set := um.groups[u.group]
	delete(set, u)
	if len(set) == 0 {
		delete(um.groups, u.group) //空组删掉，免得组名越攒越多
	}
}

// addUser 注册用户，名字已被占用返回 false。
func (um *userManager) addUser(u *user) bool {
	um.Lock()
	defer um.Unlock()
	if _, exists := um.byName[u.name]; exists {
		return false
	}
	um.users[u] = struct{}{}
	um.byName[u.name] = u //注册byName索引，方便私聊和重名检查
	um.addToGroupLocked(u)
	return true
}

func (um *userManager) removeUser(u *user) {
	um.Lock()
	defer um.Unlock()
	if _, ok := um.users[u]; !ok {
		return
	}
	delete(um.users, u)
	if um.byName[u.name] == u {
		delete(um.byName, u.name)
	}
	um.removeFromGroupLocked(u)
}

// renameUser 改名，新名字被别人占用返回 false。
func (um *userManager) renameUser(u *user, newName string) bool {
	um.Lock()
	defer um.Unlock()
	if newName == u.name {
		return true
	}
	if _, exists := um.byName[newName]; exists {
		return false
	}
	delete(um.byName, u.name)
	u.name = newName
	um.byName[newName] = u
	return true
}

// 其实就是换组
func (um *userManager) joinGroup(u *user, group string) {
	um.Lock()
	defer um.Unlock()
	if group == u.group {
		return
	}
	um.removeFromGroupLocked(u)
	u.group = group
	um.addToGroupLocked(u)
}

// 服务器向特定客户端发消息：只是入队，不阻塞、不拿 um 的锁、不直接写网络。
func (um *userManager) sendMessageToUser(u *user, message string) {
	u.send([]byte(message))
}

// 广播函数：只发给和发送者同组的人（组名空串=大厅，大厅的人互相可见）
// 持读锁的时间只有"遍历同组成员并入队"，入队不阻塞，所以锁很快就放。
func (um *userManager) broadcast(sender *user, message string) {
	um.RLock()
	line := []byte(sender.name + ": " + message + "\n") //所有接收者共用同一份只读数据
	for u := range um.groups[sender.group] {            //这里u是同组的用户*user，这里的range只用了第一个key(*user)，value是空的struct{}，因为我们只关心key
		if u != sender {
			u.send(line)
		}
	}
	um.RUnlock()
}

// handlePrivateMessage 处理私聊请求，消息格式：to|用户名|消息内容
//
// 无论成功还是失败，最后一定发送 TO_END 终止帧。客户端靠它判断
// "这条私聊已经被服务端处理完、回执也已经发出"，从而保证
// "已发送给 xxx" 一定打印在 "请输入回车键继续" 之前。
// 同一个连接的所有消息走同一个队列，所以回执和 TO_END 的先后顺序不会乱。
func (um *userManager) handlePrivateMessage(sender *user, msg string) {
	defer um.sendMessageToUser(sender, "TO_END\n")

	// 用 SplitN 限制成 3 段：消息内容里自带 "|" 时才不会被截断。
	parts := strings.SplitN(msg, "|", 3)
	if len(parts) < 3 || parts[1] == "" {
		um.sendMessageToUser(sender, "消息格式不正确，请使用 to|用户名|消息内容\n")
		return
	}
	//要发送的目标用户名
	remoteName := parts[1]
	content := parts[2]

	if content == "" {
		um.sendMessageToUser(sender, "消息内容不能为空\n")
		return
	}

	// 查找目标用户，同时在锁里把 sender.name 读出来（name 会被并发的 rename 修改）
	um.RLock()
	remoteUser := um.byName[remoteName] //map O(1)查找
	senderName := sender.name
	um.RUnlock()

	if remoteUser == nil {
		um.sendMessageToUser(sender, "用户不存在\n")
		return
	}

	// 先发给对方，再回执给发送者。remoteName 就是查询时用的名字，不用再读 remoteUser.name
	remoteUser.send([]byte(senderName + ": " + content + "\n"))
	um.sendMessageToUser(sender, "已发送给 "+remoteName+": "+content+"\n")
}

// listUsers 把在线用户拼成一整块数据一次入队，比逐行入队省队列槽位。
// 就是实现who的功能
func (um *userManager) listUsers(requester *user) {
	um.RLock()
	names := make([]string, 0, len(um.byName))
	for name := range um.byName {
		names = append(names, name)
	}
	um.RUnlock()

	sort.Strings(names) //map 遍历顺序是随机的，排个序让每次看到的编号稳定

	var sb strings.Builder //字符串拼接很贵，Builder维护了一个可变的字节切片，可以节省内存分配和拷贝，避免每次拼接都创建新的字符串
	sb.WriteString("WHO_BEGIN\n")
	for i, name := range names {
		fmt.Fprintf(&sb, "%d.%s\n", i+1, name) //这里是将用户名单写入sb中，i+1是编号，name是用户名
	}
	sb.WriteString("WHO_END\n")
	requester.send([]byte(sb.String()))
}

// readUser 读取客户端发来的用户名，创建一个 user 实例。读失败返回 nil。
func readUser(conn net.Conn, reader *bufio.Reader) *user {
	name, err := reader.ReadString('\n')
	if err != nil {
		//连上都还没报名就断了（比如探测连接），默认不打
		if verbose {
			fmt.Println("读取用户名失败:", err)
		}
		return nil
	}
	//调用newUser创建实例
	return newUser(conn, strings.TrimSpace(name))
}

func handleConnection(conn net.Conn, userManager *userManager, db *sql.DB) {
	// 不管从哪条路径退出，连接都要关（原来读用户名失败时是漏关的）
	defer conn.Close()

	// 同一个连接必须复用同一个 Reader，因为 Reader 会缓存已读取的数据。
	reader := bufio.NewReader(conn)
	currentUser := readUser(conn, reader)

	if currentUser == nil {
		return
	}

	// 注册之前还没有 writeLoop，这里直接写；写完就关，只有一行，不会卡。
	if !userManager.addUser(currentUser) {
		conn.Write([]byte("用户名已被占用，请换一个名字重新连接\n"))
		return
	}

	go currentUser.writeLoop()

	defer func() {
		userManager.removeUser(currentUser) //先摘掉，之后的广播就不会再往它的队列里塞
		userManager.broadcast(currentUser, "下线了")
		currentUser.shutdown() //通知 writeLoop 退出
	}()

	if verbose {
		fmt.Println("处理客户端请求:", conn.RemoteAddr().String())
	}

	//广播发送上线消息
	userManager.broadcast(currentUser, "上线了")
	// 持续读取客户端消息
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			//EOF是对方正常关了连接，压测收尾时一大片，默认不打
			if verbose {
				fmt.Println("读取消息失败:", err)
			}
			return
		}

		// 客户端每条消息末尾带换行；只去除协议分隔符。
		msg = strings.TrimRight(msg, "\r\n")

		if msg == "who" {
			userManager.listUsers(currentUser)
		} else if msg == "/history" {
			userManager.RLock()
			sender := currentUser.name
			userManager.RUnlock()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			msgs, err := listRecentMessages(ctx, db, sender)
			cancel()
			if err != nil {
				fmt.Println("查询历史消息失败", err)
				userManager.sendMessageToUser(currentUser, fmt.Sprintf("查询历史消息失败：%v\n", err))
				continue
			}
			for _, item := range msgs {
				userManager.sendMessageToUser(currentUser, fmt.Sprintf("历史消息 #%d %s\n", item.ID, item.Content))
			}

		} else if strings.HasPrefix(msg, "rename|") {
			newName := strings.TrimSpace(strings.SplitN(msg, "|", 2)[1])
			if newName == "" {
				userManager.sendMessageToUser(currentUser, "新用户名不能为空\n")
			} else if !userManager.renameUser(currentUser, newName) {
				userManager.sendMessageToUser(currentUser, "用户名已被占用: "+newName+"\n")
			} else {
				userManager.sendMessageToUser(currentUser, "用户名已修改为: "+newName+"\n")
			}
		} else if strings.HasPrefix(msg, "join|") {
			//join|组名 换个组；组名留空就回大厅
			group := strings.TrimSpace(msg[5:])
			userManager.joinGroup(currentUser, group)
			if group == "" {
				userManager.sendMessageToUser(currentUser, "已回到大厅\n")
			} else {
				userManager.sendMessageToUser(currentUser, "已加入组: "+group+"\n")
			}
		} else if strings.HasPrefix(msg, "to|") {
			userManager.handlePrivateMessage(currentUser, msg)
		} else {
			userManager.broadcast(currentUser, msg)
			userManager.RLock()
			sender := currentUser.name
			userManager.RUnlock()

			saveCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			id, err := saveMessage(saveCtx, db, sender, msg)
			cancel()
			if err != nil {
				fmt.Println("saveMessage错误：", err)
				continue
			}
			fmt.Println("消息id：", id)
		}
	}
}

func main() {
	//可选日志开关：想看每个连接的进出细节就加 -verbose
	flag.BoolVar(&verbose, "verbose", false, "print per-connection logs;off by default")
	flag.Parse()

	//连接MySQL
	cfg := mysql.NewConfig()
	cfg.User = "go_writer"
	cfg.Addr = "127.0.0.1"
	cfg.Net = "tcp"
	cfg.DBName = "learn_sql"
	cfg.Passwd = os.Getenv("MYSQL_WRITER_PASSWORD")
	fmt.Println("已读取密码", cfg.Passwd != "")
	fmt.Println(cfg.User, cfg.Addr, cfg.Net, cfg.DBName)

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		fmt.Println("连接器创建错误:", err)
		return
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err = db.PingContext(pingCtx); err != nil {
		fmt.Println("MySQL连接失败：", err)
		return
	}
	fmt.Println("MYSQL 连接成功")

	defer cancel()

	//启用pprof
	go func() {
		addr := "127.0.0.1:6060"
		fmt.Println("pprof listening at http://" + addr + "/debug/pprof/")
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			fmt.Println("pprof server stopped:", err)
		}
	}()
	//创建listener监听本地8080端口的tcp连接
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("监听失败:", err)
		return
	}
	defer listener.Close()

	userManager := newUserManager()

	//启用metrics采样和HTTP接口
	startMetrics(userManager)

	fmt.Println("Server 已启动，监听 :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("接收连接失败:", err)
			continue
		}

		go handleConnection(conn, userManager, db)
	}
}

func saveMessage(ctx context.Context, db *sql.DB, sender, content string) (int64, error) {

	result, err := db.ExecContext(ctx,
		"INSERT INTO messages (sender,content) VALUES (?,?)",
		sender, content)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

type Message struct {
	ID      int64
	Content string
}

func listRecentMessages(ctx context.Context, db *sql.DB, sender string) ([]Message, error) {
	var msgs []Message
	rows, err := db.QueryContext(ctx, `
	SELECT id,content
	FROM messages
	WHERE sender = ? 
	ORDER BY id DESC
	LIMIT 2
	`, sender)
	if err != nil {
		return nil, fmt.Errorf("查询消息失败：%w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message

		if err := rows.Scan(&msg.ID, &msg.Content); err != nil {
			return nil, fmt.Errorf("Scan错误：%w", err)
		}
		msgs = append(msgs, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历消息失败：%w", err)
	}

	return msgs, nil
}
