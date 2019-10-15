
pub fn div_or_zero(sum: i32, count:i32) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}
