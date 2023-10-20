use std::{env, fs};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use uuid::Uuid;
use sha256;
use tokio;
use tokio::sync::Mutex;

const CFG_DATA_DIR_WIDTH: u8 = 2;
const CFG_DATA_DIR_DEPTH: u8 = 1;

const LIST_OPERATION_BYTE: u8 = 0u8;
const PUT_OPERATION_BYTE: u8 = 1u8;
const GET_OPERATION_BYTE: u8 = 2u8;
const QUIT_OPERATION_BYTE: u8 = 3u8;
const SPUT_OPERATION_BYTE: u8 = 4u8;
const SGET_OPERATION_BYTE: u8 = 5u8;
const SIZE_OPERATION_BYTE: u8 = 6u8;
const STATS_OPERATION_BYTE: u8 = 7u8;

/// Interpret a 8 byte array as a 64 bit unsigned integer.
fn as_u64_le(array: &[u8; 8]) -> u64 {
    ((array[0] as u64) <<  0) +
        ((array[1] as u64) <<  8) +
        ((array[2] as u64) << 16) +
        ((array[3] as u64) << 24) +
        ((array[4] as u64) << 32) +
        ((array[5] as u64) << 40) +
        ((array[6] as u64) << 48) +
        ((array[7] as u64) << 56)
}

/// Cast a 64 bit unsigned integer to an 8 byte array.
fn cast_u64_to_u8_array(number: u64) -> [u8; 8] {
    return bytemuck::cast([number]);
}

fn key_to_path(key: String, data_dir_depth: u8, data_dir_width: u8) -> Result<(PathBuf, PathBuf), String> {
    let mut key_left: &str = key.as_str();
    let mut path = PathBuf::new();
    for _ in 0..data_dir_depth {
        if key_left.len() < data_dir_width as usize {
            return Err("The given key was too short.".to_string());
        }
        let (folder_name, key_last_part) = key_left.split_at(data_dir_width as usize);
        key_left = key_last_part;
        path = path.join(Path::new(folder_name));
    }
    if key_left.len() == 0 {
        return Err("The given key was too short.".to_string());
    }

    Ok((PathBuf::from(path), PathBuf::from(key_left)))
}

fn get_stored_keys_in_directory(directory: Box<Path>, current_depth: u8, data_dir_depth: u8, data_dir_width: u8) -> Vec<String> {
    if current_depth > data_dir_depth {
        return vec![];
    }

    let mut keys: Vec<String> = vec![];
    let result_read_dir = fs::read_dir(directory.clone());
    if result_read_dir.is_err() {
        return vec![];
    }

    let read_dir = result_read_dir.unwrap();

    if current_depth == data_dir_depth {
        // Get all the files in the current directory as the keys.
        for result_file in read_dir {
            if result_file.is_ok() {
                let file = result_file.unwrap();
                if file.path().is_file() {
                    let file_as_str = file.file_name().to_str().unwrap().to_owned();
                    keys.push(file_as_str);
                }
            }
        }
    } else {
        // Get all the folders in the current directory and traverse them.
        for result_folder in read_dir {
            if result_folder.is_ok() {
                let folder = result_folder.unwrap();
                if folder.path().is_dir() {
                    let folder_as_string = folder.file_name().to_str().unwrap().to_owned();
                    if folder_as_string.len() == data_dir_width as usize {
                        let all_keys_in_folder = get_stored_keys_in_directory(Box::from(directory.join(Path::new(&folder_as_string))), current_depth + 1, data_dir_depth, data_dir_width);
                        all_keys_in_folder.iter().for_each(|key| { keys.push(format!("{}{}", folder_as_string.clone(), key)); });
                    }
                }
            }
        }
    }
    keys
}

fn do_list_operation<T>(stream: &mut T, data_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(), String>
where
    T: Write
{
    let stored_keys = get_stored_keys_in_directory(data_directory, 0, data_dir_depth, data_dir_width);
    for key in stored_keys {
        if key.len() == 64 {
            let mut decoded = [0; 32];
            let decode_succeeded = hex::decode_to_slice(key, &mut decoded);
            if decode_succeeded.is_ok() {
                if stream.write(&decoded).is_err() {
                    return Err("LIST Failed to write key to stream.".to_string());
                }
            }
        }
    }
    Ok(())
}

async fn do_put_operation<T>(stream: &mut T, data_directory: Box<Path>, temporary_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(String, u64), String>
where
    T: Read + Write
{
    let temporary_file_uuid = Uuid::new_v4().to_string();
    let temporary_file_name = Path::new(&temporary_file_uuid);
    let temporary_file_location = temporary_directory.join(temporary_file_name);

    let Ok(mut temporary_file) = File::create(temporary_file_location.clone()) else {
        return Err("Failed to open temporary file.".to_string());
    };

    let mut buffer = [0u8; 1024];
    let Ok(mut read_amount) = stream.read(&mut buffer) else {
        return Err("Failed to read file stream.".to_string());
    };

    let mut total_read_amount = read_amount;

    while read_amount > 0 {
        let Ok(_) = temporary_file.write_all(&buffer[0..read_amount]) else {
            return Err("Failed to write file stream to temporary file.".to_string());
        };
        let result_read_amount = stream.read(&mut buffer);
        if result_read_amount.is_err() {
            return Err("Failed to read file stream.".to_string());
        }
        read_amount = result_read_amount.unwrap();
        total_read_amount = total_read_amount + read_amount;
    }

    let Ok(_) = temporary_file.flush() else {
        return Err("Failed to flush temporary file.".to_string());
    };

    let Ok(hash) = sha256::try_digest(temporary_file_location.clone()) else {
        return Err("Unable to make hash of temporary file.".to_string());
    };

    let Ok((final_file_directory_structure, final_file_name)) = key_to_path(hash.clone(), data_dir_depth, data_dir_width) else {
        return Err("Failed to convert file hash to directory structure.".to_string());
    };

    let final_file_location_directory = data_directory.join(final_file_directory_structure);

    if final_file_location_directory.to_str().is_some() && final_file_location_directory.to_str().unwrap().len() > 0 {
        let Ok(_) = fs::create_dir_all(final_file_location_directory.as_path()) else {
            return Err("Failed to create directory structure for file.".to_string());
        };
    }

    if fs::rename(temporary_file_location, final_file_location_directory.join(final_file_name.clone())).is_err() {
        return Err("Failed to rename the temporary file to the final file location.".to_string());
    }

    let mut decoded_hash = [0; 32];
    let Ok(_) = hex::decode_to_slice(hash.clone(), &mut decoded_hash) else {
        return Err("Failed to decode hash to bytes.".to_string());
    };

    if stream.write(&decoded_hash).is_err() {
        return Err("Failed to write key to stream.".to_string());
    }

    Ok((hash, total_read_amount as u64))
}

fn get_path_to_file(file_name: String, current_depth: u8, data_dir_depth: u8, data_dir_width: u8) -> Result<String, ()> {
    if current_depth > data_dir_depth || file_name.len() <= 0 {
        return Err(());
    }

    if current_depth == data_dir_depth {
        return Ok(file_name);
    }

    if file_name.len() <= data_dir_width as usize {
        return Err(());
    }

    let (first_part, last_part) = file_name.split_at(data_dir_width as usize);

    let result_last_part_string = get_path_to_file(last_part.to_string(), current_depth + 1, data_dir_depth, data_dir_width);

    if result_last_part_string.is_err() {
        return Err(());
    }

    let last_part_string = result_last_part_string.unwrap();
    let binding = Path::new(first_part).join(Path::new(&last_part_string.as_str()));
    let path = binding.as_path().to_str();

    match path {
        None => {
            Err(())
        }
        Some(path_as_str) => {
            Ok(path_as_str.to_string())
        }
    }
}

fn do_get_operation<T>(stream: &mut T, data_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(String, u64), String>
where
    T: Read + Write
{
    let mut key_buffer = [0u8; 32];
    let Ok(size_key_buffer_written) = stream.read(&mut key_buffer) else {
        return Err("Failed to read key.".to_string());
    };

    if size_key_buffer_written != 32 {
        return Err("Key read is not 32 bytes.".to_string());
    }

    let file_name = hex::encode(key_buffer);
    let Ok(path_to_file) = get_path_to_file(file_name.clone(), 0, data_dir_depth, data_dir_width) else {
        return Err("Failed to get path to file from given key.".to_string());
    };

    let Ok(mut file) = File::open(data_directory.join(Path::new(&path_to_file))) else {
        return Err("Failed to open file, it might not be readable or it might not exist.".to_string());
    };

    let Ok(metadata) = file.metadata() else {
        return Err("Failed to get file metadata.".to_string());
    };

    let Ok(_) = std::io::copy(&mut file, stream) else {
        return Err("Failed to write file to stream.".to_string());
    };

    Ok((file_name, metadata.len()))
}

async fn do_sput_operation<T>(stream: &mut T, data_directory: Box<Path>, temporary_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(String, u64), String>
where
    T: Read + Write
{
    let mut size_buffer = [0u8; 8];
    let Ok(bytes_read) = stream.read(&mut size_buffer) else {
        return Err("Failed to read size of SPUT operation.".to_string());
    };

    if bytes_read != 8 {
        return Err("No size specified for SPUT operation.".to_string());
    }

    // File size, currently unused.
    let _: u64 = as_u64_le(&size_buffer);
    let temporary_file_uuid = Uuid::new_v4().to_string();
    let temporary_file_name = Path::new(&temporary_file_uuid);
    let temporary_file_location = temporary_directory.join(temporary_file_name);

    let Ok(mut temporary_file) = File::create(temporary_file_location.clone()) else {
        return Err("Failed to open temporary file.".to_string());
    };

    let mut buffer = [0u8; 1024];
    let Ok(mut read_amount) = stream.read(&mut buffer) else {
        return Err("Failed to read file stream.".to_string());
    };

    let mut total_read_amount = read_amount;

    while read_amount > 0 {
        let Ok(_) = temporary_file.write_all(&buffer[0..read_amount]) else {
            return Err("Failed to write file stream to temporary file.".to_string());
        };
        let result_read_amount = stream.read(&mut buffer);
        if result_read_amount.is_err() {
            return Err("Failed to read file stream.".to_string());
        }
        read_amount = result_read_amount.unwrap();
        total_read_amount = total_read_amount + read_amount;
    }

    let Ok(_) = temporary_file.flush() else {
        return Err("Failed to flush temporary file.".to_string());
    };

    let Ok(hash) = sha256::try_digest(temporary_file_location.clone()) else {
        return Err("Unable to make hash of temporary file.".to_string());
    };

    let Ok((final_file_directory_structure, final_file_name)) = key_to_path(hash.clone(), data_dir_depth, data_dir_width) else {
        return Err("Failed to convert file hash to directory structure.".to_string());
    };

    let final_file_location_directory = data_directory.join(final_file_directory_structure);

    if final_file_location_directory.to_str().is_some() && final_file_location_directory.to_str().unwrap().len() > 0 {
        let Ok(_) = fs::create_dir_all(final_file_location_directory.as_path()) else {
            return Err("Failed to create directory structure for file.".to_string());
        };
    }

    if fs::rename(temporary_file_location, final_file_location_directory.join(final_file_name.clone())).is_err() {
        return Err("Failed to rename the temporary file to the final file location.".to_string());
    }

    let mut decoded_hash = [0; 32];
    let Ok(_) = hex::decode_to_slice(hash.clone(), &mut decoded_hash) else {
        return Err("Failed to decode hash to bytes.".to_string());
    };

    if stream.write(&decoded_hash).is_err() {
        return Err("Failed to write key to stream.".to_string());
    }

    Ok((hash, total_read_amount as u64))
}

fn do_sget_operation<T>(stream: &mut T, data_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(String, u64), String>
where
    T: Read + Write
{
    let mut key_buffer = [0u8; 32];
    let Ok(size_key_buffer_written) = stream.read(&mut key_buffer) else {
        return Err("Failed to read key.".to_string());
    };

    if size_key_buffer_written != 32 {
        return Err("Key read is not 32 bytes.".to_string());
    }

    let file_name = hex::encode(key_buffer);
    let Ok(path_to_file) = get_path_to_file(file_name.clone(), 0, data_dir_depth, data_dir_width) else {
        return Err("Failed to get path to file from given key.".to_string());
    };

    let Ok(mut file) = File::open(data_directory.join(Path::new(&path_to_file))) else {
        return Err("Failed to open file, it might not be readable or it might not exist.".to_string());
    };

    let Ok(metadata) = file.metadata() else {
        return Err("Failed to get file metadata.".to_string());
    };

    let buffer_to_write: [u8; 8] = cast_u64_to_u8_array(metadata.len());

    let Ok(_) = stream.write_all(&buffer_to_write) else {
        return Err("Failed to write size to stream.".to_string());
    };

    let Ok(_) = std::io::copy(&mut file, stream) else {
        return Err("Failed to write file to stream.".to_string());
    };

    Ok((file_name, metadata.len()))
}

fn do_size_operation<T>(stream: &mut T, data_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> Result<(String, u64), String>
where
    T: Read + Write
{
    let mut key_buffer = [0u8; 32];
    let Ok(size_key_buffer_written) = stream.read(&mut key_buffer) else {
        return Err("Failed to read key.".to_string());
    };

    if size_key_buffer_written != 32 {
        return Err("Key read is not 32 bytes.".to_string());
    }

    let file_name = hex::encode(key_buffer);
    let Ok(path_to_file) = get_path_to_file(file_name.clone(), 0, data_dir_depth, data_dir_width) else {
        return Err("SIZE Failed to get path to file from given key.".to_string());
    };

    let Ok(file) = File::open(data_directory.join(Path::new(&path_to_file))) else {
        return Err("SIZE Failed to open file, it might not be readable or it might not exist.".to_string());
    };

    let Ok(metadata) = file.metadata() else {
        return Err("SIZE Failed to get file metadata.".to_string());
    };

    let buffer_to_write: [u8; 8] = cast_u64_to_u8_array(metadata.len());

    let Ok(_) = stream.write_all(&buffer_to_write) else {
        return Err("SIZE Failed to write size to stream.".to_string());
    };

    Ok((file_name, metadata.len()))
}

async fn do_stats_operation<T>(stream: &mut T, service_data: Arc<Mutex<ServiceData>>) -> Result<(), String>
where
    T: Read + Write
{
    let service_data_lock = service_data.lock().await;
    let service_data_copy = service_data_lock.clone();
    drop(service_data_lock);

    let cycle_counter_buffer = cast_u64_to_u8_array(service_data_copy.cycle_counter);
    let bytes_sent = cast_u64_to_u8_array(service_data_copy.bytes_sent);
    let bytes_received = cast_u64_to_u8_array(service_data_copy.bytes_received);
    let connections_accepted = cast_u64_to_u8_array(service_data_copy.connections_accepted);
    let connections_active = cast_u64_to_u8_array(service_data_copy.connections_active);

    let all_buffers = [cycle_counter_buffer, bytes_sent, bytes_received, connections_accepted, connections_active];
    for buffer in all_buffers {
        let Ok(_) = stream.write_all(&buffer) else {
            return Err("Failed to write to stream.".to_string());
        };
    }

    Ok(())
}


async fn handle_stream<T>(stream: &mut T, data_directory: Box<Path>, temporary_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8, service_data: Arc<Mutex<ServiceData>>) -> bool
where
    T: Read + Write
{
    let mut operation_byte_buffer = [0u8];
    let result_first_byte_read = stream.read(&mut operation_byte_buffer);
    if result_first_byte_read.is_err() {
        println!("INITIAL Failed to read operation byte for connection");
        return false;
    }

    let first_byte_read = result_first_byte_read.unwrap();
    if first_byte_read != 1 {
        println!("INITIAL No operation byte specified.");
        return false;
    }
    let operation_byte = operation_byte_buffer[0];
    match operation_byte {
        LIST_OPERATION_BYTE => {
            match do_list_operation(stream, data_directory, data_dir_depth, data_dir_width) {
                Ok(_) => {
                    println!("LIST Operation completed successfully.");
                }
                Err(e) => {
                    println!("LIST {}", e);
                }
            }
        },
        PUT_OPERATION_BYTE => {
            match do_put_operation(stream, data_directory, temporary_directory, data_dir_depth, data_dir_width).await {
                Ok((file_name, length)) => {
                    println!("PUT File saved with key {} ({}).", file_name, length);
                    let mut service_data_lock = service_data.lock().await;
                    service_data_lock.bytes_received = service_data_lock.bytes_received + length;
                    drop(service_data_lock);
                }
                Err(e) => {
                    println!("PUT {}", e);
                }
            }
        },
        GET_OPERATION_BYTE => {
            match do_get_operation(stream, data_directory, data_dir_depth, data_dir_width) {
                Ok((file_name, length)) => {
                    println!("GET File with key {} ({}) written to stream.", file_name, length);
                    let mut service_data_lock = service_data.lock().await;
                    service_data_lock.bytes_sent = service_data_lock.bytes_sent + length;
                    drop(service_data_lock);
                }
                Err(e) => {
                    println!("GET {}", e);
                }
            }

        },
        QUIT_OPERATION_BYTE => {
            println!("QUIT");
            exit(0);
        },
        SPUT_OPERATION_BYTE => {
            match do_sput_operation(stream, data_directory, temporary_directory, data_dir_depth, data_dir_width).await {
                Ok((file_name, length)) => {
                    println!("SPUT File saved with key {} ({}).", file_name, length);
                    let mut service_data_lock = service_data.lock().await;
                    service_data_lock.bytes_received = service_data_lock.bytes_received + length;
                    drop(service_data_lock);
                }
                Err(e) => {
                    println!("SPUT {}", e);
                }
            }
        },
        SGET_OPERATION_BYTE => {
            match do_sget_operation(stream, data_directory, data_dir_depth, data_dir_width) {
                Ok((file_name, length)) => {
                    println!("SGET File with key {} ({}) written to stream.", file_name, length);
                    let mut service_data_lock = service_data.lock().await;
                    service_data_lock.bytes_sent = service_data_lock.bytes_sent + length;
                    drop(service_data_lock);
                }
                Err(e) => {
                    println!("SGET {}", e);
                }
            }
        },
        SIZE_OPERATION_BYTE => {
            match do_size_operation(stream, data_directory, data_dir_depth, data_dir_width) {
                Ok((file_name, length)) => {
                    println!("SIZE Size of file with key {} ({}) written to stream.", file_name, length);
                }
                Err(e) => {
                    println!("SIZE {}", e);
                }
            }
        },
        STATS_OPERATION_BYTE => {
            match do_stats_operation(stream, service_data.clone()).await {
                Ok(_) => {
                    println!("STATS Stats written to stream.");
                }
                Err(e) => {
                    println!("STATS {}", e);
                }
            }
        },
        _ => {
            return false;
        },
    }
    let mut service_data_lock = service_data.lock().await;
    service_data_lock.connections_accepted = service_data_lock.connections_accepted + 1;
    drop(service_data_lock);

    true
}

#[derive(Clone, Copy)]
struct ServiceData {
    cycle_counter: u64,
    bytes_sent: u64,
    bytes_received: u64,
    connections_accepted: u64,
    connections_active: u64,
}

fn start_listener(host_name: String, port: String, data_directory: Box<Path>, temporary_directory: Box<Path>, data_dir_depth: u8, data_dir_width: u8) -> bool {
    let result_listener = TcpListener::bind(format!("{}:{}", host_name, port));

    let service_data = Arc::new(Mutex::new(ServiceData {
        cycle_counter: 0,
        bytes_sent: 0,
        bytes_received: 0,
        connections_accepted: 0,
        connections_active: 0,
    }));

    if result_listener.is_err() {
        println!("BOOT Failed to start listener on {}:{}", host_name, port);
        return false;
    }
    let listener = result_listener.unwrap();
    println!("BOOT Started berthad service on {}:{}", host_name, port);

    for result_stream in listener.incoming() {
        let service_data_clone = service_data.clone();
        let data_directory_clone = data_directory.clone();
        let temporary_directory_clone = temporary_directory.clone();
        let data_dir_depth_clone = data_dir_depth.clone();
        let data_dir_width_clone = data_dir_width.clone();
        tokio::spawn(async move {
            let mut service_data_lock = service_data_clone.lock().await;
            service_data_lock.cycle_counter = service_data_lock.cycle_counter + 1;
            drop(service_data_lock);

            if result_stream.is_err() {
                println!("INITIAL Incoming stream was aborted before a connection could be established.")
            } else {
                let mut stream = result_stream.unwrap();

                let mut service_data_lock = service_data_clone.lock().await;
                service_data_lock.connections_active = service_data_lock.connections_active + 1;
                drop(service_data_lock);

                handle_stream(&mut stream, data_directory_clone, temporary_directory_clone, data_dir_depth_clone, data_dir_width_clone, service_data_clone.clone()).await;
                if stream.flush().is_err() {
                    println!("SHUTDOWN Stream flush failed.");
                }

                let mut service_data_lock = service_data_clone.lock().await;
                service_data_lock.connections_active = service_data_lock.connections_active - 1;
                drop(service_data_lock);
            }
        });
    }

    true
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        println!("Usage: berthad-vfs <bound host> <port> <data dir> <tmp dir>");
        exit(1);
    }

    let host_name = args.get(1).unwrap().clone();
    let port = args.get(2).unwrap().clone();
    let data_directory = args.get(3).unwrap().clone();
    let temporary_directory = args.get(4).unwrap().clone();

    let data_path = Path::new(&data_directory);
    let temporary_path = Path::new(&temporary_directory);

    if !data_path.exists() {
        fs::create_dir_all(data_path).expect("BOOT Failed to create data directory, do you have write access to the file system?");
    }

    if !temporary_path.exists() {
        fs::create_dir_all(temporary_path).expect("BOOT Failed to create temporary directory, do you have write access to the file system?");
    }

    start_listener(host_name, port, Box::from(data_path), Box::from(temporary_path), CFG_DATA_DIR_DEPTH, CFG_DATA_DIR_WIDTH);
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use super::*;
    use mockstream::MockStream;

    fn initialize_directories() -> (TempDir, TempDir) {
        let data_dir = TempDir::new(&Uuid::new_v4().to_string()).unwrap();
        let tmp_dir = TempDir::new(&Uuid::new_v4().to_string()).unwrap();

        (data_dir, tmp_dir)
    }

    #[test]
    fn test_byte_cast() {
        assert_eq!(123456789123456789u64, as_u64_le(&cast_u64_to_u8_array(123456789123456789u64)));
    }

    #[test]
    fn test_key_to_path() {
        assert_eq!(key_to_path("thisisalongfilename".to_string(), 0, 0), Ok((PathBuf::from("".to_string()), PathBuf::from("thisisalongfilename".to_string()))));
        assert_eq!(key_to_path("thisisalongfilename".to_string(), 4, 3), Ok((PathBuf::from("thi/sis/alo/ngf".to_string()), PathBuf::from("ilename".to_string()))));
        assert_eq!(key_to_path("thisisalongfilename".to_string(), 2, 2), Ok((PathBuf::from("th/is".to_string()), PathBuf::from("isalongfilename".to_string()))));
        assert!(key_to_path("thisisalongfilename".to_string(), 10, 4).is_err());
        assert!(key_to_path("fourfourfour".to_string(), 3, 4).is_err());
    }

    #[tokio::test]
    async fn test_get_operation() {
        let (data_dir, _) = initialize_directories();
        let file_name = "0f7783b7761110eb51bd81b122f523055e7d9e7263f61093eb29ea504aa61f90".to_string();
        let mut file = File::create(data_dir.path().join(PathBuf::from(file_name.clone()))).unwrap();
        file.write(b"These are the file contents").unwrap();
        let mut mocked_stream = MockStream::new();

        let mut decoded = [0; 32];
        hex::decode_to_slice(file_name, &mut decoded).unwrap();

        mocked_stream.push_bytes_to_read(&decoded);

        let result = do_get_operation(&mut mocked_stream, Box::from(data_dir.path()), 0, 0);
        assert!(result.is_ok());

        let file_content = mocked_stream.pop_bytes_written().clone();
        assert_eq!(file_content.as_slice(), b"These are the file contents");
    }

    #[tokio::test]
    async fn test_sget_operation() {
        let (data_dir, _) = initialize_directories();
        let file_name = "0f7783b7761110eb51bd81b122f523055e7d9e7263f61093eb29ea504aa61f90".to_string();
        let mut file = File::create(data_dir.path().join(PathBuf::from(file_name.clone()))).unwrap();
        file.write(b"These are the file contents").unwrap();
        let mut mocked_stream = MockStream::new();

        let mut decoded = [0; 32];
        hex::decode_to_slice(file_name, &mut decoded).unwrap();

        mocked_stream.push_bytes_to_read(&decoded);

        let result = do_sget_operation(&mut mocked_stream, Box::from(data_dir.path()), 0, 0);
        assert!(result.is_ok());

        let response = mocked_stream.pop_bytes_written().clone();
        let (file_size, file_content): (&[u8], &[u8]) = response.split_at(8);
        assert_eq!(file_content, b"These are the file contents");
        assert_eq!(file_size, [27, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_put_operation() {
        let (data_dir, tmp_dir) = initialize_directories();
        let mut mocked_stream = MockStream::new();
        let bytes_to_read = b"These are file contents.";
        mocked_stream.push_bytes_to_read(bytes_to_read);
        let result = do_put_operation(&mut mocked_stream, Box::from(data_dir.path()), Box::from(tmp_dir.path()), 2, 2).await;
        assert!(result.is_ok());

        let filename = mocked_stream.pop_bytes_written().clone();

        mocked_stream.flush().unwrap();

        mocked_stream.push_bytes_to_read(&filename.as_slice());
        let result = do_get_operation(&mut mocked_stream, Box::from(data_dir.path()), 2, 2);
        assert!(result.is_ok());

        assert_eq!(mocked_stream.pop_bytes_written().as_slice(), bytes_to_read);
    }

    #[tokio::test]
    async fn test_sput_operation() {
        let (data_dir, tmp_dir) = initialize_directories();
        let mut mocked_stream = MockStream::new();
        let bytes_to_read = b"These are file contents.";
        mocked_stream.push_bytes_to_read(&cast_u64_to_u8_array(bytes_to_read.len() as u64));
        mocked_stream.push_bytes_to_read(bytes_to_read);
        let result = do_sput_operation(&mut mocked_stream, Box::from(data_dir.path()), Box::from(tmp_dir.path()), 2, 2).await;
        assert!(result.is_ok());

        let filename = mocked_stream.pop_bytes_written().clone();
        mocked_stream.flush().unwrap();

        mocked_stream.push_bytes_to_read(&filename.as_slice());
        let result = do_get_operation(&mut mocked_stream, Box::from(data_dir.path()), 2, 2);
        assert!(result.is_ok());

        assert_eq!(mocked_stream.pop_bytes_written().as_slice(), bytes_to_read);
    }

    #[tokio::test]
    async fn test_size_operation() {
        let (data_dir, _) = initialize_directories();
        let file_name = "0f7783b7761110eb51bd81b122f523055e7d9e7263f61093eb29ea504aa61f90".to_string();
        let mut file = File::create(data_dir.path().join(PathBuf::from(file_name.clone()))).unwrap();
        file.write(b"These are the file contents").unwrap();
        let mut mocked_stream = MockStream::new();

        let mut decoded = [0; 32];
        hex::decode_to_slice(file_name, &mut decoded).unwrap();

        mocked_stream.push_bytes_to_read(&decoded);

        let result = do_size_operation(&mut mocked_stream, Box::from(data_dir.path()), 0, 0);
        assert!(result.is_ok());

        let file_size = mocked_stream.pop_bytes_written().clone();
        assert_eq!(file_size.as_slice(), [27, 0, 0, 0, 0, 0, 0, 0]);
    }
}