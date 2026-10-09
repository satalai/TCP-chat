package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
)

// verbose 控制每个连接的日志（接入、断开、读写失败）
// 默认关：几千个连接压测的时候这些日志会把控制台刷爆，每条Println都是实打实的I/O
var verbose bool

type user struct {
	conn  net.Conn
	name  string
	group string //所在组，空串=大厅（没进过组的用户互相可见）
}

type userManager struct {
	users []*user
	sync.RWMutex
}

func (um *userManager) addUser(u *user) {
	um.Lock()
	defer um.Unlock()
	um.users = append(um.users, u)
}

func (um *userManager) removeUser(u *user) {
	um.Lock()
	defer um.Unlock()
	for i, user := range um.users {
		if user == u {
			um.users = append(um.users[:i], um.users[i+1:]...)
			break
		}
	}
}

// 服务器向特定客户端发消息
func (um *userManager) sendMessageToUser(u *user, message string) {
	um.RLock()
	defer um.RUnlock()
	_, err := u.conn.Write([]byte(message))
	if err != nil && verbose {
		fmt.Println("发送消息失败:", err)
	}
}

// 广播函数：只发给和发送者同组的人（组名空串=大厅，大厅的人互相可见）
func (um *userManager) broadcast(sender *user, message string) {
	um.RLock()
	senderGroup := sender.group //发送者的组名先抄一份，免得和并发的join抢着改
	senderName := sender.name   //发送者的名字也抄一份，免得和并发的rename抢着改
	//同组人的连接抄成快照，循环里不再碰会变的字段
	//创建conn切片，0是当前容量，len(um.users)保证了最大数量也不会超过所有在线用户数量
	targets := make([]net.Conn, 0, len(um.users))
	for _, u := range um.users {
		if u != sender && u.group == senderGroup {
			targets = append(targets, u.conn)
		}
	}
	um.RUnlock()

	for _, conn := range targets {
		_, err := conn.Write([]byte(senderName + ": " + message + "\n"))
		if err != nil && verbose {
			fmt.Println("发送消息失败:", err)
		}
	}
}

// handlePrivateMessage 处理私聊请求，消息格式：to|用户名|消息内容
//
// 无论成功还是失败，最后一定发送 TO_END 终止帧。客户端靠它判断
// "这条私聊已经被服务端处理完、回执也已经发出"，从而保证
// "已发送给 xxx" 一定打印在 "请输入回车键继续" 之前。
func (um *userManager) handlePrivateMessage(sender *user, msg string) {
	defer um.sendMessageToUser(sender, "TO_END\n")

	// 用 SplitN 限制成 3 段：消息内容里自带 "|" 时才不会被截断。
	parts := strings.SplitN(msg, "|", 3)
	if len(parts) < 3 || parts[1] == "" {
		um.sendMessageToUser(sender, "消息格式不正确，请使用 to|用户名|消息内容\n")
		return
	}

	remoteName := parts[1]
	content := parts[2]

	if content == "" {
		um.sendMessageToUser(sender, "消息内容不能为空\n")
		return
	}

	// 查找目标用户
	var remoteUser *user
	um.RLock()
	for _, u := range um.users {
		if u.name == remoteName {
			remoteUser = u
			break
		}
	}
	um.RUnlock()

	if remoteUser == nil {
		um.sendMessageToUser(sender, "用户不存在\n")
		return
	}

	// 先发给对方，再回执给发送者
	um.sendMessageToUser(remoteUser, sender.name+": "+content+"\n")
	um.sendMessageToUser(sender, "已发送给 "+remoteUser.name+": "+content+"\n")
}

func readUser(conn net.Conn, reader *bufio.Reader) *user {
	name, err := reader.ReadString('\n')
	if err != nil {
		//连上都还没报名就断了（比如探测连接），默认不打
		if verbose {
			fmt.Println("读取用户名失败:", err)
		}
		return nil
	}

	return &user{
		conn: conn,
		name: strings.TrimSpace(name),
	}

}

func handleConnection(conn net.Conn, userManager *userManager) {

	// 同一个连接必须复用同一个 Reader，因为 Reader 会缓存已读取的数据。
	reader := bufio.NewReader(conn)
	currentUser := readUser(conn, reader)

	if currentUser == nil {
		return
	}

	defer func() {
		userManager.broadcast(currentUser, "下线了")
		userManager.removeUser(currentUser)
		conn.Close()
	}()

	userManager.addUser(currentUser)

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
			userManager.sendMessageToUser(currentUser, "WHO_BEDGIN\n")
			// 查询在线用户
			userManager.RLock()
			users := append([]*user(nil), userManager.users...)
			userManager.RUnlock()

			for i, u := range users {
				userManager.sendMessageToUser(
					currentUser,
					fmt.Sprintf("%d.%s\n", i+1, u.name),
				)
			}

			userManager.sendMessageToUser(currentUser, "WHO_END\n")
		} else if len(msg) > 7 && msg[:7] == "rename|" {
			newName := strings.Split(msg, "|")[1]
			if newName != "" {
				userManager.Lock()
				currentUser.name = newName
				userManager.Unlock()
			}
			userManager.sendMessageToUser(currentUser, "用户名已修改为: "+newName+"\n")
		} else if len(msg) >= 5 && msg[:5] == "join|" {
			//join|组名 换个组；组名留空就回大厅
			group := strings.TrimSpace(msg[5:])
			userManager.Lock()
			currentUser.group = group
			userManager.Unlock()
			if group == "" {
				userManager.sendMessageToUser(currentUser, "已回到大厅\n")
			} else {
				userManager.sendMessageToUser(currentUser, "已加入组: "+group+"\n")
			}
		} else if len(msg) >= 3 && msg[:3] == "to|" {
			userManager.handlePrivateMessage(currentUser, msg)
		} else {
			userManager.broadcast(currentUser, msg)
		}
	}
}

func main() {
	//可选日志开关：想看每个连接的进出细节就加 -verbose
	flag.BoolVar(&verbose, "verbose", false, "print per-connection logs;off by default")
	flag.Parse()

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

	var userManager userManager

	//启用metrics采样和HTTP接口
	startMetrics(&userManager)

	fmt.Println("Server 已启动，监听 :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("接收连接失败:", err)
			continue
		}

		go handleConnection(conn, &userManager)
	}
}
