package main

import (
"context"
"log"
"math/rand"
"net"
"time"

"github.com/apache/arrow/go/v14/arrow"
"github.com/apache/arrow/go/v14/arrow/array"
"github.com/apache/arrow/go/v14/arrow/flight"
"github.com/apache/arrow/go/v14/arrow/memory"
clientv3 "go.etcd.io/etcd/client/v3"
"go.etcd.io/etcd/client/v3/concurrency"
"google.golang.org/grpc"
)

type FlightServer struct {
flight.BaseFlightServer
mem memory.Allocator
}

func (s *FlightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
log.Printf("Запрос от Python: %s", string(tkt.GetTicket()))

schema := arrow.NewSchema([]arrow.Field{
{Name: "machine_id", Type: arrow.BinaryTypes.String},
{Name: "avg_temp", Type: arrow.PrimitiveTypes.Float64},
}, nil)

b := array.NewRecordBuilder(s.mem, schema)
defer b.Release()

b.Field(0).(*array.StringBuilder).Append("pump_1")
b.Field(1).(*array.Float64Builder).Append(rand.Float64() * 100)

rec := b.NewRecord()
defer rec.Release()

writer := flight.NewRecordWriter(fs, grpc.Header(nil))
writer.Write(rec)
return nil
}

func main() {
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

mutex := concurrency.NewMutex(session, "/collector/shard-1")
if err := mutex.Lock(context.Background()); err != nil {
log.Fatalf("Ошибка лока: %v", err)
}
log.Println("Шард захвачен!")

server := grpc.NewServer()
flightServer := &FlightServer{mem: memory.DefaultAllocator}
flight.RegisterFlightServiceServer(server, flightServer)

listener, err := net.Listen("tcp", ":8081")
if err != nil {
log.Fatalf("Ошибка сети: %v", err)
}

log.Println("Arrow Flight Server запущен на порту 8081")
server.Serve(listener)
}
