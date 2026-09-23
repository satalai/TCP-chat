package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"net/http"
	_ "net/http/pprof"
)

type user struct {
	conn net.Conn
	name string
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
	if err != nil {
		fmt.Println("发送消息失败:", err)
	}
}

// 广播函数
func (um *userManager) broadcast(sender *user, message string) {
	um.RLock()

	users := make([]*user, len(um.users))
	copy(users, um.users)

	um.RUnlock()

	for _, u := range users {
		if u != sender {
			_, err := u.conn.Write([]byte(sender.name + ": " + message + "\n"))
			if err != nil {
				fmt.Println("发送消息失败:", err)
			}
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
		fmt.Println("读取用户名失败:", err)
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

	fmt.Println("处理客户端请求:", conn.RemoteAddr().String())

	//广播发送上线消息
	userManager.broadcast(currentUser, "上线了")
	// 持续读取客户端消息
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("读取消息失败:", err)
			return
		}

		// 客户端每条消息末尾带换行；只去除协议分隔符。
		msg = strings.TrimRight(msg, "\r\n")
		fmt.Println(currentUser.name+":", msg)

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
		} else if len(msg) >= 3 && msg[:3] == "to|" {
			userManager.handlePrivateMessage(currentUser, msg)
		} else {
			userManager.broadcast(currentUser, msg)
		}
	}
}

func main() {
	go func(){
		addr := "127.0.0.1:6060"
		fmt.Println("pprof listening at http://" + addr + "/debug/pprof/")
		err := http.ListenAndServe(addr, nil);
		if err != nil {
        fmt.Println("pprof server stopped:", err)
    	}
	}()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("监听失败:", err)
		return
	}
	defer listener.Close()

	var userManager userManager

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
