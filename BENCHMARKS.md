# Benchmarks

Tested on AMD Ryzen 7 PRO 6850U, Linux, Go 1.26.

```bash
go test -run=^$ -bench=. -benchmem ./...
```

## Results

### 1. Chunk Reader
Reads raw chunks from network buffer.
```
BenchmarkChunkRead    ~1246 MB/s    132 KB/op    1006 allocs/op
```
*Note:* We fixed a memory issue here. We used to use `io.ReadFull` with a byte array, which made the garbage collector work too hard. Now we use `ReadByte()` directly. Memory allocations went from 4006 down to 1006.

### 2. Message Assembler
Takes the chunks and puts them together into full RTMP messages.
```
BenchmarkMessageAssemble    ~5785 MB/s    73 B/op    2 allocs/op
```
It does not create new memory for every message. It uses `sync.Pool` to reuse buffers. So it only allocates 2 times per operation.

### 3. Full Path (Chunk to Message)
This is the real loop in the server. It reads chunks and builds the message.
```
BenchmarkOwnershipChunkToMessage    ~1287 MB/s    43 KB/op    1011 allocs/op
```
*Note:* Because of our new `ReadByte()` fix, the allocations here dropped from 2511 down to 1011.

### 4. Packet Hot Path
Measures how fast we read the FLV video tag and give it to the user.
```
BenchmarkOnPacketHotPath    ~120 MB/s    ~24M pkt/s    48 B/op    1 alloc/op
```
It only creates 1 struct (`Packet`) per packet. The video data is not copied, it just points to the pooled buffer. 
We can do 24 million packets per second. A normal 1080p stream only sends about 150 packets per second, so this is good.


## How to run
```bash
# run everything
go test -run=^$ -bench=. -benchmem ./...

# run only hot path
go test -run=^$ -bench=BenchmarkOnPacketHotPath -benchmem ./rtmp

# run only full path
go test -run=^$ -bench=BenchmarkOwnershipChunkToMessage -benchmem ./internal/session
```
