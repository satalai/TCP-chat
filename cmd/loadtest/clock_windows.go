//go:build windows

package main

import (
	"syscall"
	"time"
	"unsafe"
)

// Windows上time.Now的精度很糙：这台机器实测约0.5ms才跳一次，
// 而一次socket操作就几微秒，用它量延迟不是0就是0.5ms的台阶，
// 所以改用QueryPerformanceCounter（Windows自带的计数器，本机10MHz，1 tick=100ns）
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

// nowNano返回纳秒精度的单调时间读数，取不到QPC就退回time.Now
func nowNano() int64 {
	if qpcCounter == nil {
		return time.Now().UnixNano()
	}
	return int64(float64(qpcCounter()) * qpcPerTick)
}
