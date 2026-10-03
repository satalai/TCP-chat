//go:build windows

package main

import (
	"syscall"
	"time"
	"unsafe"
)

// Windows 上 Go 的单调时钟精度很糙：这台机器上实测约 0.5ms 才跳一次，
// 而一次本机 socket 写只要几微秒，用 time.Since 量出来几乎永远是 0。
// 所以这里直接用 QueryPerformanceCounter（Windows 自带的高精度计数器，
// 本机 10MHz，1 tick = 100ns）。
var (
	qpcCounter func() int64
	qpcPerTick float64
)

func init() {
	k32 := syscall.NewLazyDLL("kernel32.dll")
	freqProc := k32.NewProc("QueryPerformanceFrequency")
	counterProc := k32.NewProc("QueryPerformanceCounter")
	var freq int64
	if r, _, _ := freqProc.Call(uintptr(unsafe.Pointer(&freq))); r == 0 || freq <= 0 {
		return
	}
	qpcPerTick = 1e9 / float64(freq)
	qpcCounter = func() int64 {
		var c int64
		counterProc.Call(uintptr(unsafe.Pointer(&c)))
		return c
	}
}

// hdNow 返回纳秒精度的单调时间读数。极端情况下取不到 QPC 就退回 time.Now。
func hdNow() int64 {
	if qpcCounter == nil {
		return time.Now().UnixNano()
	}
	return int64(float64(qpcCounter()) * qpcPerTick)
}
