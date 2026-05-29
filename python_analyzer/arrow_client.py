import pyarrow.flight as flight
import time

def test_arrow_flight():
    try:
        client = flight.FlightClient("grpc://localhost:8081")
        ticket = flight.Ticket(b"give_me_data")
        
        start = time.time()
        reader = client.do_get(ticket)
        table = reader.read_all()
        elapsed = time.time() - start
        
        print("=== Apache Arrow Flight RPC Report ===")
        print(f"Получено строк: {table.num_rows} за {elapsed:.4f} сек")
        print(f"Размер батча в памяти: {table.nbytes} байт")
        print("Схема:", table.schema)
    except Exception as e:
        print(f"Ожидание старта сервера Go... {e}")

if __name__ == "__main__":
    test_arrow_flight()
