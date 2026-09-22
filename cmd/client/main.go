package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func ensure(reader *bufio.Reader) {
	fmt.Println("请输入回车键继续")
	reader.ReadString('\n')
}

func handleSendMessage(conn net.Conn, whoDone <-chan struct{}, toDone <-chan struct{}, username string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println()
		fmt.Println("------菜单-------")
		fmt.Println("1.查询在线用户")
		fmt.Println("2.私聊模式")
		fmt.Println("3.公聊模式")
		fmt.Println("0.退出")
		fmt.Print("请选择操作:" + "\n")

		msg, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("读取消息失败:", err)
			return
		}

		msg = strings.TrimRight(msg, "\r\n")

		if msg == "0" {
			fmt.Println("退出程序")
			return
		} else if msg == "1" {
			_, err = conn.Write([]byte("who" + "\n"))
			<-whoDone
			ensure(reader)
		} else if msg == "2" {
			fmt.Println("请选择聊天用户(输入名字):")
			_, err = conn.Write([]byte("who" + "\n"))
			if err != nil{
				fmt.Println("查询在线用户失败:",err)
				continue
			}

			<-whoDone

			ToUser, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("读取用户失败:", err)
				return
			}

			ToUser = strings.TrimRight(ToUser,"\r\n")

			if ToUser == username{
				fmt.Println("不能给自己发消息")
				continue
			}

			fmt.Println("请输入私聊消息:")

			PrivateMsg, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("私聊消息读取失败:", err)
				return
			}
			PrivateMsg = strings.TrimRight(PrivateMsg, "\r\n")

			if _, err = conn.Write([]byte("to|" + ToUser + "|" + PrivateMsg + "\n")); err != nil {
				fmt.Println("发送私聊消息失败:", err)
				continue
			}

			// 关键同步点：等服务器把回执发回来、readMessage 打印完，
			// 再提示"请输入回车键继续"，保证回执不被菜单冲掉。
			<-toDone
			ensure(reader)
		} else if msg == "3" {
			fmt.Println("请输入公聊消息:")
			PublicMsg, _ := reader.ReadString('\n')
			PublicMsg = strings.TrimRight(PublicMsg, "\r\n")
			_, err = conn.Write([]byte(PublicMsg + "\n"))
			ensure(reader)
		} else {
			fmt.Println("请输入合法数字")
		}
	}
}
func readMessage(conn net.Conn, whoDone chan<- struct{}, toDone chan<- struct{}) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("读取服务器消息失败：", err)
			// 连接已断，菜单可能正阻塞在 <-whoDone / <-toDone 上，
			// 这里补发一个信号，避免永久卡死。
			select {
			case whoDone <- struct{}{}:
			default:
			}
			select {
			case toDone <- struct{}{}:
			default:
			}
			return
		}
		line = strings.TrimSpace(line)

		switch {
		case line == "WHO_BEGIN":
			fmt.Println("在线用户: ")

		case line == "WHO_END":
			whoDone <- struct{}{} //通知菜单：列表已经完整返回

		case line == "TO_END":
			toDone <- struct{}{} //通知菜单：私聊回执已经打印完

		default:
			fmt.Println(line)
		}

	}

}

func main() {
	whoDone := make(chan struct{}, 1)
	toDone := make(chan struct{}, 1)
	//创建conn连接
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("连接失败:", err)
		return
	}

	defer conn.Close()

	fmt.Println("已连接到服务器")

	// 启动一个 goroutine 来读取服务器发送的消息
	go readMessage(conn, whoDone, toDone)

	fmt.Println("请输入用户名")
	var username string
	fmt.Scanln(&username)

	_, err = conn.Write([]byte(username + "\n"))

	if err != nil {
		fmt.Println("发送用户名失败:", err)
		return
	}

	fmt.Println("用户名设置成功")

	// 启动一个 goroutine 来处理发送消息
	go handleSendMessage(conn, whoDone, toDone, username)

	// 阻塞主 goroutine，防止程序退出
	select {}
}
