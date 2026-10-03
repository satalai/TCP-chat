//go:build !windows

package main

import "time"

// 非 Windows 上 time.Now 自带的单调时钟精度够用（Linux 上纳秒级），
// 直接用它量差值。
var hdBase = time.Now()

// hdNow 返回纳秒精度的单调时间读数。
func hdNow() int64 {
	return int64(time.Since(hdBase))
}
