//! Type conversions between Rust, PostgreSQL, and Python

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyTypeError;
use tokio_postgres::Row;
use tokio_postgres::types::{ToSql, Type};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use uuid::Uuid;

/// A Python value that can be converted to PostgreSQL types
#[derive(Debug, Clone)]
pub enum PyValue {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    DateTimeUtc(DateTime<Utc>),
    List(Vec<PyValue>),
}

impl<'py> FromPyObject<'py> for PyValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_none() {
            Ok(PyValue::None)
        } else if let Ok(b) = ob.extract::<bool>() {
            Ok(PyValue::Bool(b))
        } else if let Ok(i) = ob.extract::<i64>() {
            Ok(PyValue::Int(i))
        } else if let Ok(f) = ob.extract::<f64>() {
            Ok(PyValue::Float(f))
        } else if let Ok(s) = ob.extract::<String>() {
            // Try to parse as UUID first
            if let Ok(uuid) = Uuid::parse_str(&s) {
                Ok(PyValue::Uuid(uuid))
            } else {
                Ok(PyValue::String(s))
            }
        } else if let Ok(bytes) = ob.extract::<Vec<u8>>() {
            Ok(PyValue::Bytes(bytes))
        } else if let Ok(list) = ob.downcast::<PyList>() {
            let items: PyResult<Vec<PyValue>> = list.iter().map(|item| item.extract()).collect();
            Ok(PyValue::List(items?))
        } else {
            // Try JSON serialization as fallback
            let json_mod = ob.py().import_bound("json")?;
            let json_str: String = json_mod.call_method1("dumps", (ob,))?.extract()?;
            let json_value: serde_json::Value = serde_json::from_str(&json_str)
                .map_err(|e| PyTypeError::new_err(format!("Cannot convert to JSON: {}", e)))?;
            Ok(PyValue::Json(json_value))
        }
    }
}

impl ToSql for PyValue {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PyValue::None => Ok(tokio_postgres::types::IsNull::Yes),
            PyValue::Bool(b) => b.to_sql(ty, out),
            PyValue::Int(i) => i.to_sql(ty, out),
            PyValue::Float(f) => f.to_sql(ty, out),
            PyValue::String(s) => s.to_sql(ty, out),
            PyValue::Bytes(b) => b.to_sql(ty, out),
            PyValue::Uuid(u) => u.to_sql(ty, out),
            PyValue::Json(j) => j.to_sql(ty, out),
            PyValue::Date(d) => d.to_sql(ty, out),
            PyValue::DateTime(dt) => dt.to_sql(ty, out),
            PyValue::DateTimeUtc(dt) => dt.to_sql(ty, out),
            PyValue::List(l) => {
                // Handle arrays - for simplicity, convert to JSON
                let json = serde_json::to_value(l.iter().map(|v| match v {
                    PyValue::String(s) => serde_json::Value::String(s.clone()),
                    PyValue::Int(i) => serde_json::Value::Number((*i).into()),
                    PyValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into())),
                    PyValue::Bool(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::Null,
                }).collect::<Vec<_>>()).unwrap_or(serde_json::Value::Null);
                json.to_sql(ty, out)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true // Accept all types, we'll handle conversion
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Convert a PostgreSQL row to a Python dictionary
pub fn row_to_dict<'py>(py: Python<'py>, row: &Row) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name();
        let value = column_to_pyobject(py, row, i, column.type_())?;
        dict.set_item(name, value)?;
    }
    
    Ok(dict)
}

/// Convert a single column value to a Python object
fn column_to_pyobject<'py>(py: Python<'py>, row: &Row, idx: usize, pg_type: &Type) -> PyResult<PyObject> {
    // Handle NULL values
    let raw_value: Option<&[u8]> = row.try_get(idx).ok().flatten();
    if raw_value.is_none() {
        return Ok(py.None());
    }

    match *pg_type {
        Type::BOOL => {
            let v: Option<bool> = row.get(idx);
            Ok(v.map(|b| b.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::INT2 => {
            let v: Option<i16> = row.get(idx);
            Ok(v.map(|i| i.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::INT4 => {
            let v: Option<i32> = row.get(idx);
            Ok(v.map(|i| i.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::INT8 => {
            let v: Option<i64> = row.get(idx);
            Ok(v.map(|i| i.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::FLOAT4 => {
            let v: Option<f32> = row.get(idx);
            Ok(v.map(|f| f.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::FLOAT8 | Type::NUMERIC => {
            let v: Option<f64> = row.get(idx);
            Ok(v.map(|f| f.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let v: Option<String> = row.get(idx);
            Ok(v.map(|s| s.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::BYTEA => {
            let v: Option<Vec<u8>> = row.get(idx);
            Ok(v.map(|b| b.to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::UUID => {
            let v: Option<Uuid> = row.get(idx);
            Ok(v.map(|u| u.to_string().to_object(py)).unwrap_or_else(|| py.None()))
        }
        Type::JSON | Type::JSONB => {
            let v: Option<serde_json::Value> = row.get(idx);
            match v {
                Some(json) => {
                    let json_str = serde_json::to_string(&json)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                    let json_mod = py.import_bound("json")?;
                    let py_obj = json_mod.call_method1("loads", (json_str,))?;
                    Ok(py_obj.unbind())
                }
                None => Ok(py.None()),
            }
        }
        Type::DATE => {
            let v: Option<NaiveDate> = row.get(idx);
            match v {
                Some(d) => {
                    let datetime = py.import_bound("datetime")?;
                    let date = datetime.getattr("date")?.call1((d.year(), d.month(), d.day()))?;
                    Ok(date.unbind())
                }
                None => Ok(py.None()),
            }
        }
        Type::TIME => {
            let v: Option<NaiveTime> = row.get(idx);
            match v {
                Some(t) => {
                    let datetime = py.import_bound("datetime")?;
                    let time = datetime.getattr("time")?.call1((t.hour(), t.minute(), t.second(), t.nanosecond() / 1000))?;
                    Ok(time.unbind())
                }
                None => Ok(py.None()),
            }
        }
        Type::TIMESTAMP => {
            let v: Option<NaiveDateTime> = row.get(idx);
            match v {
                Some(dt) => {
                    let datetime = py.import_bound("datetime")?;
                    let py_dt = datetime.getattr("datetime")?.call1((
                        dt.date().year(),
                        dt.date().month(),
                        dt.date().day(),
                        dt.time().hour(),
                        dt.time().minute(),
                        dt.time().second(),
                        dt.time().nanosecond() / 1000,
                    ))?;
                    Ok(py_dt.unbind())
                }
                None => Ok(py.None()),
            }
        }
        Type::TIMESTAMPTZ => {
            let v: Option<DateTime<Utc>> = row.get(idx);
            match v {
                Some(dt) => {
                    let datetime_mod = py.import_bound("datetime")?;
                    // Create datetime with timezone using fromisoformat
                    let py_dt = datetime_mod.getattr("datetime")?.call_method1(
                        "fromisoformat",
                        (dt.format("%Y-%m-%dT%H:%M:%S+00:00").to_string(),),
                    )?;
                    Ok(py_dt.unbind())
                }
                None => Ok(py.None()),
            }
        }
        _ => {
            // Fallback: try to get as string
            let v: Option<String> = row.try_get(idx).ok().flatten();
            Ok(v.map(|s| s.to_object(py)).unwrap_or_else(|| py.None()))
        }
    }
}

// Implement serde Serialize for PyValue (needed for List conversion)
impl serde::Serialize for PyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PyValue::None => serializer.serialize_none(),
            PyValue::Bool(b) => serializer.serialize_bool(*b),
            PyValue::Int(i) => serializer.serialize_i64(*i),
            PyValue::Float(f) => serializer.serialize_f64(*f),
            PyValue::String(s) => serializer.serialize_str(s),
            PyValue::Bytes(b) => serializer.serialize_bytes(b),
            PyValue::Uuid(u) => serializer.serialize_str(&u.to_string()),
            PyValue::Json(j) => j.serialize(serializer),
            PyValue::Date(d) => serializer.serialize_str(&d.to_string()),
            PyValue::DateTime(dt) => serializer.serialize_str(&dt.to_string()),
            PyValue::DateTimeUtc(dt) => serializer.serialize_str(&dt.to_string()),
            PyValue::List(l) => l.serialize(serializer),
        }
    }
}
