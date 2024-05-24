use minivec::MiniVec;
use snafu::{Backtrace, Location, Snafu};
use utils::bitset::BitSet;

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

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnData {
    pub valid: BitSet,
    pub primary_data: PrimaryColumnData,
}

impl ColumnData {
    pub fn new(column_type: PhysicalDType) -> ColumnDataResult<ColumnData> {
        let valid = BitSet::new();
        let primary_data = match column_type {
            PhysicalDType::Float => PrimaryColumnData::F64(vec![], f64::MAX, f64::MIN),
            PhysicalDType::Integer => PrimaryColumnData::I64(vec![], i64::MAX, i64::MIN),
            PhysicalDType::Unsigned => PrimaryColumnData::U64(vec![], u64::MAX, u64::MIN),
            PhysicalDType::Boolean => PrimaryColumnData::Bool(vec![], false, true),
            PhysicalDType::String => {
                PrimaryColumnData::String(vec![], String::new(), String::new())
            }
            PhysicalDType::Unknown => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build())
            }
        };
        let data = ColumnData {
            valid,
            primary_data,
        };
        Ok(data)
    }

    pub fn with_cap(column_type: PhysicalDType, cap: usize) -> ColumnDataResult<ColumnData> {
        let valid = BitSet::with_size(cap);
        let primary_data = match column_type {
            PhysicalDType::Float => {
                PrimaryColumnData::F64(Vec::with_capacity(cap), f64::MAX, f64::MIN)
            }
            PhysicalDType::Integer => {
                PrimaryColumnData::I64(Vec::with_capacity(cap), i64::MAX, i64::MIN)
            }
            PhysicalDType::Unsigned => {
                PrimaryColumnData::U64(Vec::with_capacity(cap), u64::MAX, u64::MIN)
            }
            PhysicalDType::Boolean => PrimaryColumnData::Bool(Vec::with_capacity(cap), false, true),
            PhysicalDType::String => {
                PrimaryColumnData::String(Vec::with_capacity(cap), String::new(), String::new())
            }
            PhysicalDType::Unknown => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build())
            }
        };
        let data = ColumnData {
            valid,
            primary_data,
        };
        Ok(data)
    }

    pub fn with_empty_value(
        column_type: PhysicalDType,
        len: usize,
    ) -> ColumnDataResult<ColumnData> {
        let valid = BitSet::with_size(len);
        let primary_data = match column_type {
            PhysicalDType::Float => PrimaryColumnData::F64(vec![0.0; len], f64::MAX, f64::MIN),
            PhysicalDType::Integer => PrimaryColumnData::I64(vec![0; len], i64::MAX, i64::MIN),
            PhysicalDType::Unsigned => PrimaryColumnData::U64(vec![0; len], u64::MAX, u64::MIN),
            PhysicalDType::Boolean => PrimaryColumnData::Bool(vec![false; len], false, true),
            PhysicalDType::String => {
                PrimaryColumnData::String(vec![String::new(); len], String::new(), String::new())
            }
            PhysicalDType::Unknown => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build())
            }
        };
        let data = ColumnData {
            valid,
            primary_data,
        };
        Ok(data)
    }

    pub fn get(&self, index: usize) -> Option<FieldVal> {
        if self.valid.len() <= index || self.primary_data.len() <= index {
            return None;
        }
        if self.valid.get(index) {
            self.primary_data.get(index)
        } else {
            None
        }
    }

    pub fn chunk(&self, start: usize, end: usize) -> ColumnDataResult<ColumnData> {
        let mut column = ColumnData::with_cap(self.primary_data.physical_dtype(), end - start)?;
        for index in start..end {
            column.push(self.get(index))?;
        }
        Ok(column)
    }

    pub fn push(&mut self, value: Option<FieldVal>) -> ColumnDataResult<()> {
        match (&mut self.primary_data, &value) {
            (PrimaryColumnData::F64(ref mut value, min, max), Some(FieldVal::Float(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                value.push(*val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (PrimaryColumnData::F64(ref mut value, ..), None) => {
                value.push(0.0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (PrimaryColumnData::I64(ref mut value, min, max), Some(FieldVal::Integer(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                value.push(*val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (PrimaryColumnData::I64(ref mut value, ..), None) => {
                value.push(0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (PrimaryColumnData::U64(ref mut value, min, max), Some(FieldVal::Unsigned(val))) => {
                if *max < *val {
                    *max = *val;
                }
                if *min > *val {
                    *min = *val;
                }
                value.push(*val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (PrimaryColumnData::U64(ref mut value, ..), None) => {
                value.push(0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            //todo: need to change string to Bytes type in ColumnData
            (PrimaryColumnData::String(ref mut value, min, max), Some(FieldVal::Bytes(val))) => {
                let val = String::from_utf8(val.to_vec()).unwrap();
                if *max < val {
                    *max = val.clone();
                }
                if *min > val {
                    *min = val.clone();
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (PrimaryColumnData::String(ref mut value, ..), None) => {
                value.push(String::new());
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (PrimaryColumnData::Bool(ref mut value, min, max), Some(FieldVal::Boolean(val))) => {
                if !(*max) & val {
                    *max = *val;
                }
                if *min & !val {
                    *min = *val;
                }
                value.push(*val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (PrimaryColumnData::Bool(ref mut value, ..), None) => {
                value.push(false);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            _ => {
                return Err(DataTypeMissMatchSnafu {
                    column_type: self.primary_data.physical_dtype(),
                    field_val: value.clone(),
                }
                .build())
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryColumnData {
    ///   array   min, max
    F64(Vec<f64>, f64, f64),
    I64(Vec<i64>, i64, i64),
    U64(Vec<u64>, u64, u64),
    String(Vec<String>, String, String),
    Bool(Vec<bool>, bool, bool),
}

impl PrimaryColumnData {
    pub fn get(&self, index: usize) -> Option<FieldVal> {
        return match self {
            PrimaryColumnData::F64(data, _, _) => data.get(index).map(|val| FieldVal::Float(*val)),
            PrimaryColumnData::I64(data, _, _) => {
                data.get(index).map(|val| FieldVal::Integer(*val))
            }
            PrimaryColumnData::U64(data, _, _) => {
                data.get(index).map(|val| FieldVal::Unsigned(*val))
            }
            PrimaryColumnData::String(data, _, _) => data
                .get(index)
                .map(|val| FieldVal::Bytes(MiniVec::from(val.as_str()))),
            PrimaryColumnData::Bool(data, _, _) => {
                data.get(index).map(|val| FieldVal::Boolean(*val))
            }
        };
    }

    pub fn len(&self) -> usize {
        match self {
            PrimaryColumnData::F64(data, _, _) => data.len(),
            PrimaryColumnData::I64(data, _, _) => data.len(),
            PrimaryColumnData::U64(data, _, _) => data.len(),
            PrimaryColumnData::String(data, _, _) => data.len(),
            PrimaryColumnData::Bool(data, _, _) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            PrimaryColumnData::F64(data, _, _) => data.is_empty(),
            PrimaryColumnData::I64(data, _, _) => data.is_empty(),
            PrimaryColumnData::U64(data, _, _) => data.is_empty(),
            PrimaryColumnData::String(data, _, _) => data.is_empty(),
            PrimaryColumnData::Bool(data, _, _) => data.is_empty(),
        }
    }

    /// only use for Timastamp column, other will return Err(0)
    pub fn binary_search_for_i64_col(&self, value: i64) -> ColumnDataResult<Result<usize, usize>> {
        match self {
            PrimaryColumnData::I64(data, ..) => Ok(data.binary_search(&value)),
            _ => Err(CommonSnafu {
                msg: "only use for i64 column".to_string(),
            }
            .build()),
        }
    }

    pub fn physical_dtype(&self) -> PhysicalDType {
        match self {
            PrimaryColumnData::F64(..) => PhysicalDType::Float,
            PrimaryColumnData::I64(..) => PhysicalDType::Integer,
            PrimaryColumnData::U64(..) => PhysicalDType::Unsigned,
            PrimaryColumnData::String(..) => PhysicalDType::String,
            PrimaryColumnData::Bool(..) => PhysicalDType::Boolean,
        }
    }
}
