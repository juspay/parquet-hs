// use serde::{Deserialize, Serialize};
use std::{fs, path::Path, sync::Arc};
use parquet::{column::writer::ColumnWriter, data_type::ByteArray, file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter},
    }, schema::parser::parse_message_type};
use parquet::format::FileMetaData;
use std::fs::File;
use parquet::errors::ParquetError;
use std::ffi::CString;
use std::os::raw::c_char;
use parquet::arrow::ArrowWriter;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow::datatypes::Schema;
use parquet::schema::types::ColumnPath;
use serde_json::Value;
use parquet::basic::Compression;
use std::str::FromStr;
use parquet::file::properties::BloomFilterPosition;
use parquet::file::properties::WriterPropertiesBuilder;
use arrow::record_batch::RecordBatch;
use serde_derive::Deserialize;
use serde_json::Number;
use arrow::array::{Int64Array, UInt64Array, Date32Array, Float64Array, StringArray, BooleanArray, ArrayRef, Array};
use arrow::datatypes::{Int64Type, UInt64Type, Float64Type, Date32Type };
use arrow::array::PrimitiveArray;
use indexmap::IndexMap;

type OrderedJsonMap = IndexMap<String, Value>;

pub struct ParquetSession {
    writer: ArrowWriter<File>,
    schema: Schema
}

impl ParquetSession {

    #[no_mangle]
    pub extern "C" fn new(
        schema_ptr: *const u8,
        schema_length: usize,
        file_path_ptr: *const u8,
        file_path_length: usize,
        props_ptr: *const u8,
        props_length: usize) -> *mut ParquetSession {

        unsafe {
            let schema_str = ptr_to_string(schema_ptr, schema_length);
            // let types_map = ptr_to_string(types_map_ptr, types_map_length);
            let file_path = ptr_to_string(file_path_ptr, file_path_length);
            let props = ptr_to_string(props_ptr, props_length);
            let file = fs::File::create(file_path.clone()).unwrap();
            let writer_props = Self::set_writer_props(props);
            let schema = Self::schema_from_json(schema_str.clone());
            let schema_arc = Arc::new(schema.clone());
            let arrow_writer = ArrowWriter::try_new(file, schema_arc, Some(writer_props)).unwrap();


            println!("{:?}", schema);
            Box::into_raw(Box::new(ParquetSession {
                writer: arrow_writer,
                schema: schema
            }))
        }
    }

    #[no_mangle]
    pub extern "C" fn write_batch(
        sess_ptr: *mut ParquetSession,
        batch: *const u8,
        batch_length: usize){

        unsafe {

            let ps = &mut *sess_ptr;
            let batch_string = ptr_to_string(batch, batch_length);
            let rb = Self::create_record_batch(ps.schema.clone(), batch_string);

            ps.writer.write(&rb).unwrap();
        }
    }

    #[no_mangle]
    pub extern "C" fn flush_row_group(
        sess_ptr: *mut ParquetSession){
        unsafe {
            let ps = &mut *sess_ptr;
            println!("flushing");
            ps.writer.flush().unwrap();
        }
    }

    #[no_mangle]
    pub extern "C" fn close_writer(
        sess_ptr: *mut ParquetSession){
        unsafe {
            let ps = Box::from_raw(sess_ptr);
            println!("closing writer");
            ps.writer.close().unwrap();
        }
    }

    //helper functions

    pub fn set_writer_props(props: String) -> WriterProperties { // remove pub after testing
        // properties keys are setter funtion names as in WriterPropertiesBuilder

        let empty = vec![];

        let props_json : Value = serde_json::from_str(props.as_str()).unwrap();
        let bloom_filter_position_str = props_json.get("set_bloom_filter_position").and_then(|v| v.as_str()).unwrap_or_else(|| {
            println!("set_bloom_filter_position: Key not found, using default as AfterRowGroup");
            "AfterRowGroup"
        });
        let compression = Compression::from_str(props_json.get("set_compression").and_then(|v| v.as_str()).unwrap_or_else(|| {
            println!("set_compression: Key not found, using default as UNCOMPRESSED");
            "UNCOMPRESSED"
        })).unwrap();
        let enable_bloom_filter = props_json.get("set_bloom_filter_enabled").and_then(|v| v.as_bool()).unwrap_or_else(|| {
            println!("enable_bloom_filter: Key not found, using default as false");
            false
        });
        let compression_col_names = props_json.get("set_compression_column_names").and_then(|v| v.get("columns")).and_then(|v| v.as_array()).unwrap_or_else(|| {
            println!("set_compression_column_names : Key not found, not setting any compression encodings for columns");
            &empty
        });
        let compression_col_encodings = props_json.get("set_compression_column_encodings").and_then(|v| v.get("encodings")).and_then(|v| v.as_array()).unwrap_or_else(|| {
            println!("set_compression_column_encodings : Key not found, not setting any compression encodings for columns");
            &empty
        });

        let max_row_group_size = props_json.get("set_max_row_group_size").and_then(|v| v.as_u64()).unwrap_or(100000);

        let bloom_filter_position = match bloom_filter_position_str {
            "End" => BloomFilterPosition::End,
            _ => BloomFilterPosition::AfterRowGroup,
        };

        let mut props = WriterProperties::builder();
        props = props.set_bloom_filter_enabled(enable_bloom_filter);
        props = props.set_bloom_filter_position(bloom_filter_position);
        props = props.set_compression(compression);
        props = props.set_max_row_group_size(max_row_group_size.try_into().unwrap());

        if compression_col_names.len() == compression_col_encodings.len() {
            for i in 0..compression_col_names.len(){
                let c = Compression::from_str(compression_col_encodings[i].as_str().unwrap_or("UNCOMPRESSED")).unwrap();
                props = props.set_column_compression(ColumnPath::from(compression_col_names[i].to_string()), c);
            }
        }
        else {
            println!("Couldn't set column encoding as number of cols doesn't match given number of encodings");
        }

        println!("Properties Set");
        props.build()
    }

    pub fn schema_from_json(schema_str: String) -> Schema{
        // schema is of form:
        // {
        //   column_name : {
        //            datatype: ..
        //            nullable: ..
        //           }
        //   .
        //   .
        //   .
        // }
        //
        //
        let schema_json: OrderedJsonMap = serde_json::from_str(schema_str.as_str()).unwrap();

        println!("{:?}", schema_json);

        // let schema_json: Value = serde_json::from_str(schema_str.as_str()).unwrap();

        let mut result: Vec<(String, String, bool)> = Vec::new();

        // if let Value::Object(map) = schema_json {
            for (key, inner_val) in schema_json {
                if let Value::Object(inner_map) = inner_val {
                    let datatype = inner_map.get("datatype").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let nullable = inner_map.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                    result.push((key, datatype, nullable));
                }
            }
        // }

        let schema_fields : Vec<Field> = result.into_iter().map(|(col_name, col_type_str, nullable)| {
            Self::create_schema_field(col_name, col_type_str, nullable)}).collect();
        Schema::new(schema_fields)
    }

    fn create_schema_field(col_name: String, col_type_str: String, nullable: bool) -> Field {
        let col_type: DataType = col_type_str.parse().unwrap();
        Field::new(col_name, col_type, nullable)
    }

    pub fn create_record_batch(schema: Schema, batch: String) -> RecordBatch{
        let columnar: Vec<Vec<Value>> = serde_json::from_str(batch.as_str()).unwrap();

        // let columnar = Self::transpose(rows);

        let fields = schema.fields();
        let types: Vec<&DataType> = fields.into_iter().map(|f| {
            f.data_type()
        }).collect();

        let columns: Vec<Arc<dyn Array>> = (0..columnar.len())
            .map(|i| {
                Self::types_to_arrow_array(columnar[i].clone(), types[i].clone())
            }).collect();

        println!("{:?}", columns);
        println!("{:?}", types);

        RecordBatch::try_new(
            Arc::new(schema),
            columns
        ).unwrap()
    }

    fn transpose(mut v: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        assert!(!v.is_empty());
        for inner in &mut v {
            inner.reverse();
        }
        (0..v[0].len())
            .map(|_| {
                v.iter_mut()
                 .map(|inner| inner.pop().unwrap())
                 .collect::<Vec<Value>>()
            })
            .collect()
    }

    fn types_to_arrow_array(column: Vec<Value>, col_type: DataType) -> ArrayRef {

        match col_type {
            DataType::Int64 => {
                let col: PrimitiveArray<Int64Type> = column.into_iter().map(|v| {
                    v.as_i64()
                }).collect();
                Arc::new(Int64Array::from(col)) as ArrayRef
            }
            DataType::UInt64 => {
                let col: PrimitiveArray<UInt64Type> = column.into_iter().map(|v| {
                    v.as_u64()
                }).collect();
                Arc::new(UInt64Array::from(col)) as ArrayRef
            }
            DataType::Float64 => {
                let col: PrimitiveArray<Float64Type> = column.into_iter().map(|v| {
                    v.as_f64()
                }).collect();
                Arc::new(Float64Array::from(col)) as ArrayRef
            }
            DataType::Utf8 => {
                let col_vec: Vec<Option<String>>  = column.into_iter().map(|v| {
                    let mut entry = v.to_string();
                    entry.trim_ascii();
                    entry.make_ascii_uppercase();
                    if entry == "NULL"{
                        None
                    }
                    else {
                        Some(entry)
                    }
                }).collect();
                Arc::new(StringArray::from(col_vec)) as ArrayRef
            }

            // DataType::Date32 => {
            //     let col: PrimitiveArray<Date32Type> = column.into_iter().map(|v| {
            //         v.to_string()
            //     }).collect();
            //     Arc::new(Date32Array::from(col)) as ArrayRef
            // }

            DataType::Boolean => {
                let col: Vec<Option<bool>> = column.into_iter().map(|v| {
                    v.as_bool()
                }).collect();

                Arc::new(BooleanArray::from(col)) as ArrayRef
            }

            _ =>  {
                let col: Vec<Option<String>> = column.into_iter().map(|_| {
                   None
                }).collect();

                Arc::new(StringArray::from(col)) as ArrayRef
            }
        }
    }

}

unsafe fn ptr_to_string(ptr: *const u8, len: usize) -> String {
    std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)).to_owned()
}

// #[no_mangle]
// pub extern "C" fn init_parquet_writer(
//     message_type_ptr: *const u8,
//     message_type_length: usize,
//     types_map_ptr: *const u8,
//     types_map_length: usize,
//     file_path_ptr: *const u8,
//     file_path_length: usize) -> *mut SerializedFileWriter<File> {

//     // if message_type_ptr.is_null() || types_map_ptr.is_null() || file_path_ptr.is_null() {
//     //     return Err(ParquetError::General(format!("pointer pass were null")))
//     // }

//     unsafe {
//         let message_type = ptr_to_string(message_type_ptr, message_type_length);
//         let types_map = ptr_to_string(types_map_ptr, types_map_length);
//         let file_path = ptr_to_string(file_path_ptr, file_path_length);

//         let schema = Arc::new(parse_message_type(message_type.as_str()).unwrap());
//         let props = Arc::new(WriterProperties::builder().build());
//         let file = fs::File::create(file_path).unwrap();

//         let writer = SerializedFileWriter::new(file, schema, props).unwrap();
//         Box::into_raw(Box::new(writer))
//     }

// }


// // #[no_mangle]
// // pub extern "C" fn write_row<'a>(message_type: &'a str, types: &'a [&'a str], file_path: &'a str) -> SerializedFileWriter<File> {
// // }

// // #[no_mangle]
// // pub extern "C" fn flush_writer<'a>(message_type: &'a str, types: &'a [&'a str], file_path: &'a str) -> SerializedFileWriter<File> {
// // }

// #[no_mangle]
// pub extern "C" fn close_writer(writer: SerializedFileWriter<File>) -> Result<FileMetaData, ParquetError>{
//     writer.close()
// }



// pub fn add(left: u64, right: u64) -> u64 {
//     left + right
// }
