# 1 billion row challenge golang


1. First try, no optimizations (8 cores CPU, buffered reading with worker pool) - ~5m 27s
2. Optimizations (concurrent calculations, pass chunks of strings to goroutines instead of line by line) ~ 1m 50s