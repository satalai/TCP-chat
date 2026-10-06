//go:build !windows

package main

import "time"

// 非Windows（Linux/Mac）的time.Now本身就是纳秒精度，直接用
var clockBase = time.Now()

// nowNano返回纳秒精度的单调时间读数
func nowNano() int64 {
	return int64(time.Since(clockBase))
}
