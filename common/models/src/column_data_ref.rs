use arrow_buffer::builder::BooleanBufferBuilder;
use snafu::{Backtrace, Location, Snafu};

use crate::field_value::FieldVal;
use crate::PhysicalDType;

pub type ColumnDataResult<T, E = ColumnDataError> = Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ColumnDataError {
    #[snafu(display("Unsupport data type: {}", dt))]
    UnsupportedDataType {
        dt: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Data type miss match: column type: {:?}, field_val: {:?}",
        column_type,
        field_val
    ))]
    DataTypeMissMatch {
        column_type: PhysicalDType,
        field_val: Option<FieldVal>,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("MutableColumnError: {}", msg))]
    CommonError {
        msg: String,
        location: Location,
        backtrace: Backtrace,
    },
}
#[derive(Debug)]
pub struct ColumnDataRef<'a> {
    pub valid: BooleanBufferBuilder,
    pub primary_data: PrimaryColumnDataRef<'a>,
}

impl<'a> Clone for ColumnDataRef<'a> {
    fn clone(&self) -> Self {
        let values = self.valid.as_slice();
        let len = self.valid.len();
        let mut new_valid = BooleanBufferBuilder::new(len);
        new_valid.append_packed_range(0..len, values);

        ColumnDataRef {
            valid: new_valid,
            primary_data: self.primary_data.clone(),
        }
    }
}

impl<'a> PartialEq for ColumnDataRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.valid.as_slice() == other.valid.as_slice()
            && self.valid.len() == other.valid.len()
            && self.primary_data == other.primary_data
    }
}

impl<'a> ColumnDataRef<'a> {
    pub fn new(column_type: PhysicalDType, len: usize) -> ColumnDataResult<ColumnDataRef<'a>> {
        let valid = BooleanBufferBuilder::new(len);
        let primary_data = match column_type {
            PhysicalDType::Float => PrimaryColumnDataRef::F64(vec![0.0; len], f64::MAX, f64::MIN),
            PhysicalDType::Integer => PrimaryColumnDataRef::I64(vec![0; len], i64::MAX, i64::MIN),
            PhysicalDType::Unsigned => PrimaryColumnDataRef::U64(vec![0; len], u64::MAX, u64::MIN),
            PhysicalDType::Boolean => PrimaryColumnDataRef::Bool(vec![false; len], false, true),
            PhysicalDType::String => {
                PrimaryColumnDataRef::String(vec!["".as_bytes(); len], "".as_bytes(), "".as_bytes())
            }
            PhysicalDType::Unknown => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build())
            }
        };
        let data = ColumnDataRef {
            valid,
            primary_data,
        };
        Ok(data)
    }

    pub fn push_ts(&mut self, val: i64) -> ColumnDataResult<()> {
        match &mut self.primary_data {
            PrimaryColumnDataRef::I64(ref mut values, min, max) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                values.push(val);
                self.valid.append(true);
            }

            _ => {
                return Err(DataTypeMissMatchSnafu {
                    column_type: self.primary_data.physical_dtype(),
                    field_val: Some(FieldVal::Integer(val)),
                }
                .build())
            }
        }
        Ok(())
    }

    pub fn push(&mut self, value: Option<&'a FieldVal>) -> ColumnDataResult<()> {
        match (&mut self.primary_data, value) {
            (PrimaryColumnDataRef::F64(ref mut values, min, max), Some(FieldVal::Float(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append(true);
            }
            (PrimaryColumnDataRef::F64(..), None) => {
                self.valid.append(false);
            }

            (PrimaryColumnDataRef::I64(ref mut values, min, max), Some(FieldVal::Integer(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append(true);
            }
            (PrimaryColumnDataRef::I64(..), None) => {
                self.valid.append(false);
            }

            (
                PrimaryColumnDataRef::U64(ref mut values, min, max),
                Some(FieldVal::Unsigned(val)),
            ) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append(true);
            }
            (PrimaryColumnDataRef::U64(..), None) => {
                self.valid.append(false);
            }

            (
                PrimaryColumnDataRef::String(ref mut values, min, max),
                Some(FieldVal::Bytes(val)),
            ) => {
                let val = val.as_slice();
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                values.push(val);
                self.valid.append(true);
            }
            (PrimaryColumnDataRef::String(..), None) => {
                self.valid.append(false);
            }

            (
                PrimaryColumnDataRef::Bool(ref mut values, min, max),
                Some(FieldVal::Boolean(val)),
            ) => {
                if !(*max) & val {
                    *max = *val;
                }
                if *min & !val {
                    *min = *val;
                }
                values.push(*val);
                self.valid.append(true);
            }
            (PrimaryColumnDataRef::Bool(..), None) => {
                self.valid.append(false);
            }

            _ => {
                return Err(DataTypeMissMatchSnafu {
                    column_type: self.primary_data.physical_dtype(),
                    field_val: value.cloned(),
                }
                .build())
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryColumnDataRef<'a> {
    ///   array   min, max
    F64(Vec<f64>, f64, f64),
    I64(Vec<i64>, i64, i64),
    U64(Vec<u64>, u64, u64),
    String(Vec<&'a [u8]>, &'a [u8], &'a [u8]),
    Bool(Vec<bool>, bool, bool),
}

impl<'a> PrimaryColumnDataRef<'a> {
    pub fn physical_dtype(&self) -> PhysicalDType {
        match self {
            PrimaryColumnDataRef::F64(..) => PhysicalDType::Float,
            PrimaryColumnDataRef::I64(..) => PhysicalDType::Integer,
            PrimaryColumnDataRef::U64(..) => PhysicalDType::Unsigned,
            PrimaryColumnDataRef::String(..) => PhysicalDType::String,
            PrimaryColumnDataRef::Bool(..) => PhysicalDType::Boolean,
        }
    }
}
