#[no_mangle]
pub extern "C" fn validate_telemetry(temp: f64, vibration: f64) -> i32 {
    // Валидация: температура 0-150, вибрация 0-100
    if temp >= 0.0 && temp <= 150.0 && vibration >= 0.0 && vibration <= 100.0 {
        1 // True
    } else {
        0 // False
    }
}
