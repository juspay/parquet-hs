use std::{fs, sync::Arc};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use parquet::arrow::ArrowWriter;
use arrow_schema::{DataType, TimeUnit};
use arrow_schema::Field;
use arrow::datatypes::Schema;
use parquet::schema::types::ColumnPath;
use serde_json::Value;
use parquet::basic::Compression;
use std::str::FromStr;
use parquet::file::properties::BloomFilterPosition;
use arrow::record_batch::RecordBatch;
use arrow::array::{
    PrimitiveBuilder, BooleanBuilder, StringBuilder,
    ListBuilder, Int64Array, UInt64Array,
    Float64Array, StringArray, BooleanArray,
    ArrayRef, Array, TimestampMicrosecondArray, ListArray};
use arrow::datatypes::{
    ArrowPrimitiveType,Int64Type, UInt64Type,
    Float64Type, ArrowTimestampType, TimestampMicrosecondType};
use arrow::array::PrimitiveArray;
use indexmap::IndexMap;
use chrono::NaiveDateTime;

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

    fn set_writer_props(props: String) -> WriterProperties {
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

        let max_row_group_size = props_json.get("set_max_row_group_size").and_then(|v| v.as_u64()).unwrap_or(4056);

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
        //
        let schema_json: OrderedJsonMap = serde_json::from_str(schema_str.as_str()).unwrap();

        let mut result: Vec<(String, String, bool)> = Vec::new();

        for (key, inner_val) in schema_json {
            if let Value::Object(inner_map) = inner_val {
                let datatype = inner_map.get("datatype").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let nullable = inner_map.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                result.push((key, datatype, nullable));
            }
        }

        let schema_fields : Vec<Field> = result.into_iter().map(|(col_name, col_type_str, nullable)| {
            Self::create_schema_field(col_name, col_type_str, nullable)}).collect();
        Schema::new(schema_fields)
    }

    fn create_schema_field(col_name: String, col_type_str: String, nullable: bool) -> Field {
        let col_type: DataType = col_type_str.parse().unwrap_or(DataType::Utf8);
        Field::new(col_name, col_type, nullable)
    }

    fn create_record_batch(schema: Schema, batch: String) -> RecordBatch{
        let columnar: Vec<Vec<Value>> = serde_json::from_str(batch.as_str()).unwrap();

        let fields = schema.fields();
        let types: Vec<&DataType> = fields.into_iter().map(|f| {
            f.data_type()
        }).collect();

        let columns: Vec<Arc<dyn Array>> = (0..columnar.len())
            .map(|i| {
                Self::types_to_arrow_array(columnar[i].clone(), types[i].clone())
            }).collect();

        RecordBatch::try_new(
            Arc::new(schema),
            columns
        ).unwrap()
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
                    let mut entry = v.as_str().unwrap().to_string();
                    entry = entry.trim_ascii().to_string();
                    if entry.to_ascii_uppercase() == "NULL"{
                        None
                    }
                    else {
                        Some(entry.to_string())
                    }
                }).collect();
                Arc::new(StringArray::from(col_vec)) as ArrayRef
            }

            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let col: PrimitiveArray<TimestampMicrosecondType> = column.into_iter().map(|v| {
                    match NaiveDateTime::parse_from_str(v.as_str().unwrap_or("null"), "%FT%T%.fZ")
                        .or_else(|_|
                        NaiveDateTime::parse_from_str(v.as_str().unwrap_or("null"), "%F %T%.f"))
                    {
                        Ok(ndt) => TimestampMicrosecondType::make_value(ndt),
                        Err(e) => {
                            println!("{:?}", e);
                            None
                        }
                    }
                }).collect();
                Arc::new(TimestampMicrosecondArray::from(col)) as ArrayRef
            }

            DataType::Boolean => {
                let col: Vec<Option<bool>> = column.into_iter().map(|v| {
                    v.as_bool()
                }).collect();

                Arc::new(BooleanArray::from(col)) as ArrayRef
            }

            DataType::List(field) => {
                let datatype = field.data_type();
                match datatype {
                    DataType::Utf8 => {
                        let mut builder = ListBuilder::new(StringBuilder::new());
                        for v in column {
                            if v.is_array() {
                                for inner_v in v.as_array().unwrap() {
                                    match inner_v.as_str() {
                                        Some(s) => {
                                            builder.values().append_value(s);
                                        }
                                        None => {
                                            builder.values().append_null();
                                        }
                                    }
                                }
                            }
                            else {
                                builder.values().append_null();
                            }
                            builder.append(true);
                        };
                        Arc::new(ListArray::from(builder.finish())) as ArrayRef
                    }

                    DataType::Int64 => {
                        let builder = PrimitiveBuilder::new().with_data_type(DataType::Int64);
                        to_list_array::<Int64Type>(builder, column, Value::as_i64)
                    }

                    DataType::UInt64 => {
                        let builder = PrimitiveBuilder::new().with_data_type(DataType::UInt64);
                        to_list_array::<UInt64Type>(builder, column, Value::as_u64)
                    }

                    DataType::Float64 => {
                        let builder = PrimitiveBuilder::new().with_data_type(DataType::Float64);
                        to_list_array::<Float64Type>(builder, column, Value::as_f64)
                    }

                    DataType::Boolean => {
                        let mut builder = ListBuilder::new(BooleanBuilder::new());
                        for v in column {
                            if v.is_array() {
                                for inner_v in v.as_array().unwrap() {
                                    match inner_v.as_bool() {
                                        Some(s) => {
                                            builder.values().append_value(s);
                                        }
                                        None => {
                                            builder.values().append_null();
                                        }
                                    }
                                }
                            }
                            else {
                                builder.values().append_null();
                            }
                            builder.append(true);
                        };
                        Arc::new(ListArray::from(builder.finish())) as ArrayRef
                    }
                    _ => todo!()
                }
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

fn to_list_array<T : ArrowPrimitiveType>(builder: PrimitiveBuilder<T>, column : Vec<Value>, f : fn(&Value) -> Option<T::Native> ) -> Arc<dyn Array>{
    let mut list_builder = ListBuilder::new(builder);
    for v in column {
        if v.is_array() {
            for inner_v in v.as_array().unwrap() {
                match f(inner_v) {
                    Some(s) => {
                        list_builder.values().append_value(s);
                    }
                    None => {
                        list_builder.values().append_null();
                    }
                }
            }
        }
        else {
            list_builder.values().append_null();
        }
        list_builder.append(true);
    };
    Arc::new(list_builder.finish()) as ArrayRef
}

unsafe fn ptr_to_string(ptr: *const u8, len: usize) -> String {
    std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)).to_owned()
}
