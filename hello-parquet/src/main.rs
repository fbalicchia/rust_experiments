fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {

    use parquet::{
        column::{reader::ColumnReader, writer::ColumnWriter},
        data_type::Int32Type,
        file::{
            properties::WriterProperties,
            reader::{FileReader, SerializedFileReader},
            writer::SerializedFileWriter,
        },
        schema::parser::parse_message_type,
    };

    use std::{any::Any, fs, path::Path, sync::Arc};

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    // fn parquet_test() {
    //     let path = Path::new("./sample.parquet");

    //     let message_type = "
    //         message schema {
    //             REQUIRED INT32 b;
    //             REQUIRED BINARY msg (UTF8);
    //         }
    //     ";
    //     let schema = Arc::new(parse_message_type(message_type).unwrap());
    //     let props = Arc::new(WriterProperties::builder().build());
    //     let file = fs::File::create(&path).unwrap();

    //     let mut rows: i64 = 0;
    //     let data = vec![(10, "A"), (20, "B"), (30, "C"), (40, "D")];

    //     let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    //     for (key, value) in data {
    //         let mut row_group_writer = writer.next_row_group().unwrap();
    //         let id_writer = row_group_writer.next_column().unwrap();
    //         if let Some(mut writer) = id_writer {
    //             match writer {
    //                 ColumnWriter::Int32ColumnWriter(ref mut typed) => {
    //                     let values = vec![key];
    //                     rows += typed.write_batch(&values[..], None, None).unwrap() as i64;
    //                 }
    //                 _ => {
    //                     unimplemented!();
    //                 }
    //             }
    //             row_group_writer.close_column(writer).unwrap();
    //         }
    //         let data_writer = row_group_writer.next_column().unwrap();
    //         if let Some(mut writer) = data_writer {
    //             match writer {
    //                 ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
    //                     let values = ByteArray::from(value);
    //                     rows += typed.write_batch(&[values], None, None).unwrap() as i64;
    //                 }
    //                 _ => {
    //                     unimplemented!();
    //                 }
    //             }
    //             row_group_writer.close_column(writer).unwrap();
    //         }
    //         writer.close_row_group(row_group_writer).unwrap();
    //     }
    //     writer.close().unwrap();

    //     println!("Wrote {}", rows);

    //     let bytes = fs::read(&path).unwrap();
    //     assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    // }

    #[test]
    fn sample_writing() {
        let path = Path::new("./sample.parquet");
        let message_type = "
            message schema {
                REQUIRED INT32 b;
                REQUIRED BINARY msg (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(&path).unwrap();
        let data = vec![(10, "A"), (20, "B"), (30, "C"), (40, "D")];





        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            col_writer
                .typed::<Vec<(i32,&str)>()
                .write_batch(&data[..], Some(&[3, 3, 3, 2, 2]), Some(&[0, 1, 0, 1, 1]))
                .unwrap();
            col_writer.close().unwrap();
        }
        row_group_writer.close().unwrap();

        writer.close().unwrap();


        
    }

    #[test]
    fn write_api_parquet() {
        let path = Path::new("./column_sample.parquet");

        let message_type = "
        message schema {
            optional group values (LIST) {
            repeated group list {
                optional INT32 element;
            }
            }
        }
        ";

        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            col_writer
                .typed::<Int32Type>()
                .write_batch(&[1, 2, 3], Some(&[3, 3, 3, 2, 2]), Some(&[0, 1, 0, 1, 1]))
                .unwrap();
            col_writer.close().unwrap();
        }
        row_group_writer.close().unwrap();

        writer.close().unwrap();
    }

    #[test]
    fn read_api_paruet() {
        let path = Path::new("./column_sample.parquet");

        let file = fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();

        let mut res = Ok((0, 0));
        let mut values = vec![0; 8];
        let mut def_levels = vec![0; 8];
        let mut rep_levels = vec![0; 8];

        for i in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let row_group_metadata = metadata.row_group(i);

            for j in 0..row_group_metadata.num_columns() {
                let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
                match column_reader {
                    // You can also use `get_typed_column_reader` method to extract typed reader.
                    ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                        res = typed_reader.read_batch(
                            8, // batch size
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values,
                        );
                    }
                    _ => {}
                }
            }
        }

        assert_eq!(res, Ok((3, 5)));
        assert_eq!(values, vec![1, 2, 3, 0, 0, 0, 0, 0]);
        assert_eq!(def_levels, vec![3, 3, 3, 2, 2, 0, 0, 0]);
        assert_eq!(rep_levels, vec![0, 1, 0, 1, 1, 0, 0, 0]);
    }
}
