package main

import (
"fmt"
"math/rand"
"os"
"os/signal"
"syscall"
"time"

"github.com/parquet-go/parquet-go"
)

type MachineMetrics struct {
Timestamp    int64   `parquet:"timestamp,timestamp(millisecond)"`
MachineID    string  `parquet:"machine_id,string"`
AvgTemp      float64 `parquet:"avg_temperature,double"`
MaxPressure  float64 `parquet:"max_pressure,double"`
AvgVibration float64 `parquet:"avg_vibration,double"`
MsgCount     int64   `parquet:"msg_count,int64"`
}

type RawSignal struct {
MachineID   string
Temperature float64
Pressure    float64
Vibration   float64
Timestamp   time.Time
}

func main() {
fmt.Println("=== Go Industrial Collector Started ===")

rawChan := make(chan RawSignal, 500)
stopChan := make(chan os.Signal, 1)
signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

go func() {
machines := []string{"CNC-01", "CNC-02", "ROBOT-A", "PRESS-05"}
r := rand.New(rand.NewSource(time.Now().UnixNano()))
for {
for _, mId := range machines {
rawChan <- RawSignal{
MachineID:   mId,
Temperature: 60.0 + r.Float64()*40.0,
Pressure:    2.0 + r.Float64()*3.0,
Vibration:   10.0 + r.Float64()*50.0,
Timestamp:   time.Now(),
}
}
time.Sleep(250 * time.Millisecond)
}
}()

go func() {
windowDuration := 5 * time.Second
ticker := time.NewTicker(windowDuration)
windowData := make(map[string][]RawSignal)
filePath := "/data/industrial_metrics.parquet"

for {
select {
case sig := <-rawChan:
windowData[sig.MachineID] = append(windowData[sig.MachineID], sig)

case <-ticker.C:
if len(windowData) == 0 {
continue
}

file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
if err != nil {
fmt.Printf("Ошибка открытия Parquet файла: %v\n", err)
continue
}

writer := parquet.NewWriter(file, parquet.SchemaOf(MachineMetrics{}))
fmt.Printf("[%s] Сброс окна агрегации в Parquet...\n", time.Now().Format("15:04:05"))

for mID, signals := range windowData {
var sumTemp, maxPress, sumVib float64
for i, s := range signals {
sumTemp += s.Temperature
sumVib += s.Vibration
if i == 0 || s.Pressure > maxPress {
maxPress = s.Pressure
}
}
count := int64(len(signals))

record := MachineMetrics{
Timestamp:    time.Now().UnixMilli(),
MachineID:    mID,
AvgTemp:      sumTemp / float64(count),
MaxPressure:  maxPress,
AvgVibration: sumVib / float64(count),
MsgCount:     count,
}

if err := writer.Write(record); err != nil {
fmt.Printf("Ошибка записи в Parquet: %v\n", err)
}
}

writer.Close()
file.Close()
windowData = make(map[string][]RawSignal)
}
}
}()

<-stopChan
fmt.Println("=== Go Industrial Collector Gracefully Stopped ===")
}
