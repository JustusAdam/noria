use noria::{DataType};

pub struct Z(pub i32);

impl From<DataType> for Z {
    fn from(other: DataType) -> Self {
        match other {
            DataType::None => Z(0),
            DataType::Int(i) => Z(i),
            _ => unreachable!("{:?}", other),
        }
    }
}

pub fn div_or_zero(sum0: Z, count0:Z) -> f64 {
    let count = count0.0;
    let sum = sum0.0;
    if count == 0 {
        0.0
    } else {
        //eprintln!("sum: {}, count: {}", sum, count);
        sum as f64 / count as f64
    }
}
