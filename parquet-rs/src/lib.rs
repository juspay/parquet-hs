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

// #[derive(Serialize, Deserialize, Debug)]
// pub struct WriterSession {
//   writer: SerializedFileWriter<File>,
// }
//
//
//
//
// #[derive(Deserialize)]
// pub enum Types {
//     Null,
//     Bool(bool),
//     Number(Number),
//     String(String),
// }

struct ParquetSession {
    writer: ArrowWriter<File>,
    schema: String
}

impl ParquetSession {

    #[no_mangle]
    pub extern "C" fn new(
        schema_ptr: *const u8,
        schema_length: usize,
        types_map_ptr: *const u8,
        types_map_length: usize,
        file_path_ptr: *const u8,
        file_path_length: usize,
        props_ptr: *const u8,
        props_length: usize) -> *mut ParquetSession {

        // if message_type_ptr.is_null() || types_map_ptr.is_null() || file_path_ptr.is_null() {
        //     return Err(ParquetError::General(format!("pointer pass were null")))
        // }

        unsafe {
            let schema_str = ptr_to_string(schema_ptr, schema_length);
            let types_map = ptr_to_string(types_map_ptr, types_map_length);
            let file_path = ptr_to_string(file_path_ptr, file_path_length);
            let props_str = ptr_to_string(props_ptr, props_length);
            let props_json = serde_json::from_str(props_str.as_str());

            let props = Arc::new(WriterProperties::builder().build());
            let file = fs::File::create(file_path).unwrap();

            Self::set_writer_props(props_json.unwrap());
            let schema = Arc::new(Self::schema_from_json(schema_str.clone()));
            let arrow_writer = ArrowWriter::try_new(file, schema, None).unwrap();

            Box::into_raw(Box::new(ParquetSession {writer: arrow_writer,
                                                   schema: schema_str
                                                              }))
        }
    }

    // #[no_mangle]
    // pub extern "C" fn write_batch(
    //     sess_ptr: *mut ParquetSession,
    //     batch: *const u8,
    //     batch_length: *const u8) -> Result<FileMetaData, ParquetError>{


    // }


    //helper functions

    fn set_writer_props(props: String) -> WriterProperties {
        // properties keys are setter funtion names as in WriterPropertiesBuilder

        let props_json : Value = serde_json::from_str(props.as_str()).unwrap();
        let bloom_filter_position_str = props_json.get("set_bloom_filter_position").and_then(|v| v.as_str()).unwrap_or("AfterRowGroup");
        let compression = Compression::from_str(props_json.get("set_compression").and_then(|v| v.as_str()).unwrap_or("UNCOMPRESSED")).unwrap();
        let enable_bloom_filter = props_json.get("set_bloom_filter_enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        let compression_col_names = props_json.get("set_column_compression").and_then(|v| v.get("columns")).and_then(|v| v.as_array()).unwrap();
        let compression_col_encoding = props_json.get("set_column_compression").and_then(|v| v.get("encodings")).and_then(|v| v.as_array()).unwrap();
        let max_row_group_size = props_json.get("set_max_row_group_size").and_then(|v| v.as_u64()).unwrap();

        let bloom_filter_position = match bloom_filter_position_str {
            "End" => BloomFilterPosition::End,
            _ => BloomFilterPosition::AfterRowGroup,
        };

        let mut props = WriterProperties::builder();
        props = props.set_bloom_filter_enabled(enable_bloom_filter);
        props = props.set_bloom_filter_position(bloom_filter_position);
        props = props.set_compression(compression);
        props = props.set_max_row_group_size(max_row_group_size.try_into().unwrap());


        for i in 0..compression_col_names.len(){
            let c = Compression::from_str(compression_col_encoding[i].as_str().unwrap_or("UNCOMPRESSED")).unwrap();
            props = props.set_column_compression(ColumnPath::from(compression_col_names[i].to_string()), c);
        }

        props.build()
    }

    fn schema_from_json(schema_str: String) -> Schema{
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

        let schema_json: Value = serde_json::from_str(schema_str.as_str()).unwrap();

        let mut result: Vec<(String, String, bool)> = Vec::new();

        if let Value::Object(map) = schema_json {
            for (key, inner_val) in map {
                if let Value::Object(inner_map) = inner_val {
                    let a = inner_map.get("a").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let b = inner_map.get("b").and_then(|v| v.as_bool()).unwrap_or(false);
                    result.push((key, a, b));
                }
            }
        }

        let schema_fields : Vec<Field> = result.into_iter().map(|(col_name, col_type_str, nullable)| {
            Self::create_schema_field(col_name, col_type_str, nullable)}).collect();

        Schema::new(schema_fields)
    }

    fn create_schema_field(col_name: String, col_type_str: String, nullable: bool) -> Field {
        // convert string to datatype
        let col_type: DataType = col_type_str.parse().unwrap();
        Field::new(col_name, col_type, nullable)
    }

    fn create_record_batch(schema: Schema, batch: String) -> RecordBatch{
        let rows: Vec<Vec<Value>> = serde_json::from_str(batch.as_str()).unwrap();

        let columnar = transpose(rows);

        let rec_batch = RecordBatch::try_new(
            Arc::new(schema),
            rows
        );
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


    // fn get

    // #[no_mangel]
    // pub extern "C" fn create_writer(ps_ptr: *mut ParquetSession){
    //     unsafe {
    //         let mut ps = unsafe{Box::from_raw(ps_ptr)};

    //         let writer
    //      }
    // }

    //  #[no_mangle]
    //  pub extern "C" fn write_batch(
    //      ps_ptr: *mut ParquetSession,
    //      batch_path_ptr: *const u8,
    //      batch_path_length: usize) {

    //      unsafe {
    //          let mut ps = unsafe{Box::from_raw(ps_ptr)};
    //          let mut row_group_writer = ps.writer.next_row_group().unwrap();
    //          let id_writer = row_group_writer.next_column().unwrap();
    //          let batch_file_path = ptr_to_string(batch_path_ptr, batch_path_length);




    //      }





    //      // [1, 2, 3].into_iter().map(|x| x + 1).rev().collect();

    //      // remember to flush rgw
    //      // row_group_writer.close()

    //  }


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
