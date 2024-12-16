use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array, UInt64Builder,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use models::schema::tskv_table_schema::{PhysicalCType, TskvTableSchemaRef};
use models::PhysicalDType as ValueType;
use protos::models::{
    Column as FbColumn, ColumnBuilder, ColumnType as FbColumnType, FieldType, PointsBuilder,
    TableBuilder, ValuesBuilder,
};

use crate::{Error, FieldValue, Line, Result};

struct TableData {
    times: Int64Builder,
    tags: HashMap<String, StringBuilder>,
    fields: HashMap<String, FieldBuilder>,
    row_count: usize,
}

enum FieldBuilder {
    U64(UInt64Builder),
    I64(Int64Builder),
    F64(Float64Builder),
    String(StringBuilder),
    Bool(BooleanBuilder),
}

impl FieldBuilder {
    fn append_option(&mut self, value: Option<&FieldValue>) -> ArrowResult<()> {
        match (&mut *self, value) {
            (FieldBuilder::U64(builder), Some(FieldValue::U64(v))) => {
                builder.append_value(*v);
                Ok(())
            }
            (FieldBuilder::I64(builder), Some(FieldValue::I64(v))) => {
                builder.append_value(*v);
                Ok(())
            }
            (FieldBuilder::F64(builder), Some(FieldValue::F64(v))) => {
                builder.append_value(*v);
                Ok(())
            }
            (FieldBuilder::String(builder), Some(FieldValue::Str(v))) => {
                match std::str::from_utf8(v) {
                    Ok(s) => {
                        builder.append_value(s);
                        Ok(())
                    }
                    Err(_) => Err(ArrowError::InvalidArgumentError(
                        "Invalid string value".to_string(),
                    )),
                }
            }
            (FieldBuilder::Bool(builder), Some(FieldValue::Bool(v))) => {
                builder.append_value(*v);
                Ok(())
            }
            (_, None) => match self {
                FieldBuilder::U64(ref mut builder) => {
                    builder.append_null();
                    Ok(())
                }
                FieldBuilder::I64(ref mut builder) => {
                    builder.append_null();
                    Ok(())
                }
                FieldBuilder::F64(ref mut builder) => {
                    builder.append_null();
                    Ok(())
                }
                FieldBuilder::String(ref mut builder) => {
                    builder.append_null();
                    Ok(())
                }
                FieldBuilder::Bool(ref mut builder) => {
                    builder.append_null();
                    Ok(())
                }
            },
            _ => Err(ArrowError::InvalidArgumentError(
                "Type mismatch between builder and value".to_string(),
            )),
        }
    }

    fn len(&self) -> usize {
        match self {
            FieldBuilder::U64(b) => b.len(),
            FieldBuilder::I64(b) => b.len(),
            FieldBuilder::F64(b) => b.len(),
            FieldBuilder::String(b) => b.len(),
            FieldBuilder::Bool(b) => b.len(),
        }
    }
}

struct ColumnInfo {
    array: ArrayRef,
    name: String,
    field_type: FieldType,
    column_type: FbColumnType,
}

impl TableData {
    fn new() -> Self {
        Self {
            times: Int64Builder::new(),
            tags: HashMap::new(),
            fields: HashMap::new(),
            row_count: 0,
        }
    }

    fn add_line(&mut self, line: &Line) -> ArrowResult<()> {
        self.times.append_value(line.timestamp);

        for (tag_key, tag_value) in &line.tags {
            let builder = self.tags.entry(tag_key.to_string()).or_default();
            while builder.len() < self.row_count {
                builder.append_null();
            }
            builder.append_value(tag_value);
        }

        for (field_key, field_value) in &line.fields {
            let builder =
                self.fields
                    .entry(field_key.to_string())
                    .or_insert_with(|| match field_value {
                        FieldValue::U64(_) => FieldBuilder::U64(UInt64Builder::new()),
                        FieldValue::I64(_) => FieldBuilder::I64(Int64Builder::new()),
                        FieldValue::F64(_) => FieldBuilder::F64(Float64Builder::new()),
                        FieldValue::Str(_) => FieldBuilder::String(StringBuilder::new()),
                        FieldValue::Bool(_) => FieldBuilder::Bool(BooleanBuilder::new()),
                    });
            while builder.len() < self.row_count {
                builder.append_option(None)?;
            }
            builder.append_option(Some(field_value))?;
        }

        self.row_count += 1;

        for tag_builder in self.tags.values_mut() {
            while tag_builder.len() < self.row_count {
                tag_builder.append_null();
            }
        }
        for field_builder in self.fields.values_mut() {
            while field_builder.len() < self.row_count {
                field_builder.append_option(None)?;
            }
        }

        Ok(())
    }

    fn finish(mut self) -> Result<Vec<ColumnInfo>, ArrowError> {
        let mut columns = Vec::new();

        columns.push(ColumnInfo {
            array: Arc::new(self.times.finish()),
            name: "time".to_string(),
            field_type: FieldType::Integer,
            column_type: FbColumnType::Time,
        });

        let mut tags: Vec<_> = self.tags.into_iter().collect();
        tags.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (name, mut builder) in tags {
            columns.push(ColumnInfo {
                array: Arc::new(builder.finish()),
                name,
                field_type: FieldType::String,
                column_type: FbColumnType::Tag,
            });
        }

        let mut fields: Vec<_> = self.fields.into_iter().collect();
        fields.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (name, mut builder) in fields {
            let (field_type, array) = match &mut builder {
                FieldBuilder::U64(b) => (FieldType::Unsigned, Arc::new(b.finish()) as ArrayRef),
                FieldBuilder::I64(b) => (FieldType::Integer, Arc::new(b.finish()) as ArrayRef),
                FieldBuilder::F64(b) => (FieldType::Float, Arc::new(b.finish()) as ArrayRef),
                FieldBuilder::String(b) => (FieldType::String, Arc::new(b.finish()) as ArrayRef),
                FieldBuilder::Bool(b) => (FieldType::Boolean, Arc::new(b.finish()) as ArrayRef),
            };
            columns.push(ColumnInfo {
                array,
                name,
                field_type,
                column_type: FbColumnType::Field,
            });
        }

        Ok(columns)
    }
}

pub fn line_to_point(lines: &[Line], db: &str) -> Result<Vec<u8>, Error> {
    let fbb = &mut FlatBufferBuilder::new();
    let mut tables_map = HashMap::new();
    for line in lines {
        let table_data = tables_map
            .entry(line.table.to_string())
            .or_insert_with(TableData::new);

        table_data.add_line(line).map_err(|e| Error::Common {
            content: format!("Failed to add line: {}", e),
        })?;
    }

    let mut fb_tables = Vec::new();
    for (table_name, table_data) in tables_map {
        let row_count = table_data.row_count;
        let columns = table_data.finish().map_err(|e| Error::Common {
            content: format!("Failed to finish table: {}", e),
        })?;

        let mut fb_columns = Vec::new();
        for column in columns {
            let column_name = fbb.create_string(column.name.as_str());

            let nulls = column.array.nulls();
            let nullbits = match nulls {
                Some(nulls) => nulls.buffer().as_slice().to_vec(),
                None => {
                    let nullbits = NullBuffer::new_valid(column.array.len());
                    nullbits.buffer().as_slice().to_vec()
                }
            };
            let nullbits = fbb.create_vector(&nullbits);
            let column_values = match column.array.data_type() {
                DataType::UInt64 => {
                    let values = column
                        .array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| Error::Common {
                            content: "Failed to downcast to UInt64Array".to_string(),
                        })?;
                    let col_values: Vec<u64> = values.iter().map(|v| v.unwrap_or(0)).collect();
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_uint_value(values);
                    values_builder.finish()
                }
                DataType::Int64 => {
                    let values = column
                        .array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| Error::Common {
                            content: "Failed to downcast to Int64Array".to_string(),
                        })?;
                    let col_values: Vec<i64> = values.iter().map(|v| v.unwrap_or(0)).collect();
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_int_value(values);
                    values_builder.finish()
                }
                DataType::Float64 => {
                    let values = column
                        .array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| Error::Common {
                            content: "Failed to downcast to Float64Array".to_string(),
                        })?;
                    let col_values: Vec<f64> = values.iter().map(|v| v.unwrap_or(0.0)).collect();
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_float_value(values);
                    values_builder.finish()
                }
                DataType::Utf8 => {
                    let values = column
                        .array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| Error::Common {
                            content: "Failed to downcast to StringArray".to_string(),
                        })?;
                    let mut col_values = Vec::with_capacity(values.len());
                    for value in values.iter() {
                        let str_offset = if let Some(value) = value {
                            fbb.create_string(value)
                        } else {
                            fbb.create_string("")
                        };
                        col_values.push(str_offset);
                    }
                    let values = fbb.create_vector(&col_values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_string_value(values);
                    values_builder.finish()
                }
                DataType::Boolean => {
                    let values = column
                        .array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| Error::Common {
                            content: "Failed to downcast to BooleanArray".to_string(),
                        })?;
                    let values: Vec<bool> = values.iter().map(|v| v.unwrap_or(false)).collect();
                    let values = fbb.create_vector(&values);
                    let mut values_builder = ValuesBuilder::new(fbb);
                    values_builder.add_bool_value(values);
                    values_builder.finish()
                }
                _ => {
                    return Err(Error::Common {
                        content: "Unsupported data type".to_string(),
                    });
                }
            };
            let mut column_builder = ColumnBuilder::new(fbb);
            column_builder.add_field_type(column.field_type);
            column_builder.add_column_type(column.column_type);
            column_builder.add_name(column_name);
            column_builder.add_nullbits(nullbits);
            column_builder.add_col_values(column_values);
            fb_columns.push(column_builder.finish());
        }
        let columns = fbb.create_vector(&fb_columns);
        let table_name = fbb.create_string(&table_name);
        let mut table_builder = TableBuilder::new(fbb);
        table_builder.add_columns(columns);
        table_builder.add_tab(table_name);
        table_builder.add_num_rows(row_count as u64);
        fb_tables.push(table_builder.finish());
    }
    let tables = fbb.create_vector(&fb_tables);
    let db = fbb.create_string(db);
    let mut point_builder = PointsBuilder::new(fbb);
    point_builder.add_tables(tables);
    point_builder.add_db(db);
    let points = point_builder.finish();
    fbb.finish(points, None);
    let data = fbb.finished_data().to_vec();
    Ok(data)
}

pub fn arrow_array_to_points(
    columns: Vec<ArrayRef>,
    schema: SchemaRef,
    table_schema: TskvTableSchemaRef,
    len: usize,
) -> Result<Vec<u8>> {
    let mut fbb = FlatBufferBuilder::new();
    let table_name = table_schema.name.as_str();
    let mut fb_columns = Vec::new();
    for (column, schema) in columns.iter().zip(schema.fields.iter()) {
        let col_name = schema.name().as_str();
        let column_schema = table_schema.column(col_name).ok_or_else(|| Error::Common {
            content: format!("column {} not found in table {}", col_name, table_name),
        })?;
        let fb_column = match column_schema.column_type.to_physical_type() {
            PhysicalCType::Tag => {
                build_string_column(column, col_name, FbColumnType::Tag, &mut fbb)?
            }
            PhysicalCType::Time(ref time_unit) => {
                build_timestamp_column(column, col_name, time_unit, &mut fbb)?
            }
            PhysicalCType::Field(value_type) => match value_type {
                ValueType::Unknown => {
                    return Err(Error::Common {
                        content: format!("column {} type is unknown", col_name),
                    });
                }
                ValueType::Float => build_f64_column(column, col_name, &mut fbb)?,
                ValueType::Integer => build_i64_column(column, col_name, &mut fbb)?,
                ValueType::Unsigned => build_u64_column(column, col_name, &mut fbb)?,
                ValueType::Boolean => build_bool_column(column, col_name, &mut fbb)?,
                ValueType::String => {
                    build_string_column(column, col_name, FbColumnType::Field, &mut fbb)?
                }
            },
        };
        fb_columns.push(fb_column);
    }
    let columns = fbb.create_vector(&fb_columns);
    let table_name = fbb.create_string(table_name);
    let mut table_builder = TableBuilder::new(&mut fbb);
    table_builder.add_columns(columns);
    table_builder.add_tab(table_name);
    table_builder.add_num_rows(len as u64);
    let table = table_builder.finish();
    let tables = fbb.create_vector(&[table]);
    let db = fbb.create_string(table_schema.db.as_str());
    let mut point_builder = PointsBuilder::new(&mut fbb);
    point_builder.add_tables(tables);
    point_builder.add_db(db);
    let points = point_builder.finish();
    fbb.finish(points, None);
    let data = fbb.finished_data().to_vec();
    Ok(data)
}

pub fn build_string_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fb_column_type: FbColumnType,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Common {
            content: format!("column {} is not string", col_name),
        })?;

    let col_values: Vec<_> = array
        .iter()
        .map(|value| fbb.create_string(value.unwrap_or("")))
        .collect();
    let nullbits = array
        .nulls()
        .map(|nulls| nulls.buffer().as_slice().to_vec())
        .unwrap_or_else(|| {
            NullBuffer::new_valid(array.len())
                .buffer()
                .as_slice()
                .to_vec()
        });
    let nullbits = fbb.create_vector(&nullbits);

    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_string_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(fb_column_type);
    column_builder.add_field_type(FieldType::String);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_timestamp_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    time_unit: &TimeUnit,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let (nullbits, col_values) = match time_unit {
        TimeUnit::Second => {
            return Err(Error::Common {
                content: "time column not support second".to_string(),
            });
        }
        TimeUnit::Millisecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let nullbits = values
                .nulls()
                .map(|nulls| nulls.buffer().as_slice().to_vec())
                .unwrap_or_else(|| {
                    NullBuffer::new_valid(values.len())
                        .buffer()
                        .as_slice()
                        .to_vec()
                });
            let col_values: Vec<i64> = values.iter().map(|value| value.unwrap_or(0)).collect();
            (nullbits, col_values)
        }
        TimeUnit::Microsecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let nullbits = values
                .nulls()
                .map(|nulls| nulls.buffer().as_slice().to_vec())
                .unwrap_or_else(|| {
                    NullBuffer::new_valid(values.len())
                        .buffer()
                        .as_slice()
                        .to_vec()
                });
            let col_values: Vec<i64> = values.iter().map(|value| value.unwrap_or(0)).collect();
            (nullbits, col_values)
        }
        TimeUnit::Nanosecond => {
            let values = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Common {
                    content: format!("column {} is not int64", col_name),
                })?;
            let nullbits = values
                .nulls()
                .map(|nulls| nulls.buffer().as_slice().to_vec())
                .unwrap_or_else(|| {
                    NullBuffer::new_valid(values.len())
                        .buffer()
                        .as_slice()
                        .to_vec()
                });
            let col_values: Vec<i64> = values.iter().map(|value| value.unwrap_or(0)).collect();
            (nullbits, col_values)
        }
    };
    let nullbits = fbb.create_vector(&nullbits);
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_int_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Time);
    column_builder.add_field_type(FieldType::Integer);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_i64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Common {
            content: format!("column {} is not int64", col_name),
        })?;
    let nullbits = values
        .nulls()
        .map(|nulls| nulls.buffer().as_slice().to_vec())
        .unwrap_or_else(|| {
            NullBuffer::new_valid(values.len())
                .buffer()
                .as_slice()
                .to_vec()
        });
    let col_values: Vec<i64> = values.iter().map(|value| value.unwrap_or(0)).collect();
    let nullbits = fbb.create_vector(&nullbits);
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_int_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Integer);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_f64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Common {
            content: format!("column {} is not float64", col_name),
        })?;
    let nullbits = values
        .nulls()
        .map(|nulls| nulls.buffer().as_slice().to_vec())
        .unwrap_or_else(|| {
            NullBuffer::new_valid(values.len())
                .buffer()
                .as_slice()
                .to_vec()
        });
    let col_values: Vec<f64> = values.iter().map(|value| value.unwrap_or(0.0)).collect();
    let nullbits = fbb.create_vector(&nullbits);
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_float_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Float);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_u64_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| Error::Common {
            content: format!("column {} is not uint64", col_name),
        })?;

    let nullbits = values
        .nulls()
        .map(|nulls| nulls.buffer().as_slice().to_vec())
        .unwrap_or_else(|| {
            NullBuffer::new_valid(values.len())
                .buffer()
                .as_slice()
                .to_vec()
        });
    let col_values: Vec<u64> = values.iter().map(|value| value.unwrap_or(0)).collect();
    let nullbits = fbb.create_vector(&nullbits);
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_uint_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Unsigned);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}

pub fn build_bool_column<'a>(
    column: &ArrayRef,
    col_name: &str,
    fbb: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<FbColumn<'a>>> {
    let name = fbb.create_string(col_name);
    let values = column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Common {
            content: format!("column {} is not bool", col_name),
        })?;
    let nullbits = values
        .nulls()
        .map(|nulls| nulls.buffer().as_slice().to_vec())
        .unwrap_or_else(|| {
            NullBuffer::new_valid(values.len())
                .buffer()
                .as_slice()
                .to_vec()
        });
    let col_values: Vec<bool> = values.iter().map(|value| value.unwrap_or(false)).collect();
    let nullbits = fbb.create_vector(&nullbits);
    let values = fbb.create_vector(&col_values);
    let mut values_builder = ValuesBuilder::new(fbb);
    values_builder.add_bool_value(values);
    let values = values_builder.finish();
    let mut column_builder = ColumnBuilder::new(fbb);
    column_builder.add_name(name);
    column_builder.add_column_type(FbColumnType::Field);
    column_builder.add_field_type(FieldType::Boolean);
    column_builder.add_nullbits(nullbits);
    column_builder.add_col_values(values);
    Ok(column_builder.finish())
}
