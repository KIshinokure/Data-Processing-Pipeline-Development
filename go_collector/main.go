package main

import (
"context"
"encoding/json"
"fmt"
"log"
"math/rand"
"net"
"os"
"sync"
"time"

"github.com/apache/arrow/go/v14/arrow"
"github.com/apache/arrow/go/v14/arrow/array"
"github.com/apache/arrow/go/v14/arrow/flight"
"github.com/apache/arrow/go/v14/arrow/memory"
"github.com/segmentio/kafka-go"
clientv3 "go.etcd.io/etcd/client/v3"
"go.etcd.io/etcd/client/v3/concurrency"
"google.golang.org/grpc"
)

/*
#cgo LDFLAGS: -L./validator/target/release -lvalidator
int validate_telemetry(double temp, double vibration);
*/
import "C"

type WindowAggregator struct {
mu      sync.Mutex
TempSum float64
VibSum  float64
Count   int64
}

type FlightServer struct {
flight.BaseFlightServer
mem memory.Allocator
agg *WindowAggregator
}

func (s *FlightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
s.agg.mu.Lock()
defer s.agg.mu.Unlock()

schema := arrow.NewSchema([]arrow.Field{
{Name: "avg_temp", Type: arrow.PrimitiveTypes.Float64},
{Name: "msg_count", Type: arrow.PrimitiveTypes.Int64},
}, nil)

b := array.NewRecordBuilder(s.mem, schema)
defer b.Release()

if s.agg.Count > 0 {
b.Field(0).(*array.Float64Builder).Append(s.agg.TempSum / float64(s.agg.Count))
b.Field(1).(*array.Int64Builder).Append(s.agg.Count)
}

rec := b.NewRecord()
defer rec.Release()
writer := flight.NewRecordWriter(fs, grpc.Header(nil))
return writer.Write(rec)
}

func main() {
rand.Seed(time.Now().UnixNano())
agg := &WindowAggregator{}

podName := os.Getenv("POD_NAME")
if podName == "" {
podName = "local-instance"
}

cli, err := clientv3.New(clientv3.Config{
Endpoints:   []string{"etcd:2379"},
DialTimeout: 5 * time.Second,
})
if err != nil {
log.Fatalf("Ошибка etcd: %v", err)
}
defer cli.Close()

session, _ := concurrency.NewSession(cli)
defer session.Close()

shardPath := fmt.Sprintf("/collector/shards/%s", podName)
mutex := concurrency.NewMutex(session, shardPath)

ctx := context.Background()
if err := mutex.Lock(ctx); err != nil {
log.Fatalf("Не удалось захватить шард: %v", err)
}
log.Printf("Шард %s захвачен", shardPath)
defer mutex.Unlock(ctx)

kafkaWriter := &kafka.Writer{
Addr:     kafka.TCP("kafka:9092"),
Topic:    "telemetry",
Balancer: &kafka.LeastBytes{},
}
defer kafkaWriter.Close()

ticker := time.NewTicker(5 * time.Second)
go func() {
for range ticker.C {
agg.mu.Lock()
if agg.Count > 0 {
avgTemp := agg.TempSum / float64(agg.Count)
msg := map[string]interface{}{
"timestamp": time.Now().Format(time.RFC3339),
"avg_temp":  avgTemp,
"count":     agg.Count,
}
msgBytes, _ := json.Marshal(msg)

kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: msgBytes})
log.Printf("Отправлено в Kafka окно: %d записей, Ср.Темп: %.2f", agg.Count, avgTemp)

agg.TempSum = 0
agg.VibSum = 0
agg.Count = 0
}
agg.mu.Unlock()
}
}()

go func() {
for {
t := rand.Float64() * 120.0
v := rand.Float64() * 60.0

if C.validate_telemetry(C.double(t), C.double(v)) == 1 {
agg.mu.Lock()
agg.TempSum += t
agg.VibSum += v
agg.Count++
agg.mu.Unlock()
}
time.Sleep(100 * time.Millisecond)
}
}()

server := grpc.NewServer()
flight.RegisterFlightServiceServer(server, &FlightServer{mem: memory.DefaultAllocator, agg: agg})
listener, _ := net.Listen("tcp", ":8081")
log.Println("Arrow Flight Server запущен на 8081")
server.Serve(listener)
}
