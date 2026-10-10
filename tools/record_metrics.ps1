# Usage: .\tools\record_metrics.ps1 results\before_g10.csv
# Polls /metrics once per second and appends a CSV row. Ctrl+C to stop.
param([string]$Out = "metrics.csv")

$header = "time,online,goroutines,threads,proc_cpu,sys_cpu,rss_mb,heap_alloc_mb,heap_inuse_mb,heap_sys_mb,stack_inuse_mb,num_gc,mallocs_per_sec,alloc_mb_per_sec,gc_per_sec,gc_pause_ms"
Set-Content -Path $Out -Value $header -Encoding UTF8

Write-Host "recording to $Out, Ctrl+C to stop"
while ($true) {
    try {
        $m = Invoke-RestMethod http://127.0.0.1:8081/metrics -TimeoutSec 2
        $line = @(
            $m.time, $m.online_users, $m.goroutines, $m.threads,
            $m.cpu.process_percent, $m.cpu.system_percent,
            $m.memory.rss_mb, $m.memory.go_heap_alloc_mb, $m.memory.go_heap_inuse_mb,
            $m.memory.go_heap_sys_mb, $m.memory.go_stack_inuse_mb, $m.memory.go_num_gc,
            $m.alloc.mallocs_per_sec, $m.alloc.alloc_mb_per_sec, $m.alloc.gc_per_sec,
            $m.alloc.gc_pause_ms_in_window
        ) -join ","
        Add-Content -Path $Out -Value $line -Encoding UTF8
    } catch {
        Write-Host "fetch failed: $_"
    }
    Start-Sleep -Seconds 1
}