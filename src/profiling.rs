use crate::frontend::JobInfo;
use datafusion::common::Result;
use serde_json;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

/// Appends a single `JobInfo` to a JSON file, creating the file if it does not exist.
/// Ensures that the data is flushed to disk.
///
/// # Arguments
/// * `job` - The `JobInfo` to append to the file.
/// * `path` - The file path to which the `JobInfo` should be appended.
///
/// # Returns
/// A `Result<(), serde_json::Error>` indicating success or failure.
pub async fn append_job_to_json_file(job: &JobInfo, path: &Path) -> Result<(), serde_json::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .await
        .expect("Unable to open or create file");

    let json_string = serde_json::to_string_pretty(&job)?;

    let file_len = file
        .metadata()
        .await
        .expect("Unable to read metadata")
        .len();

    let mut buf_writer = BufWriter::new(&mut file);

    if file_len == 0 {
        buf_writer
            .write_all(b"[")
            .await
            .expect("Unable to write data to file");
        buf_writer
            .write_all(json_string.as_bytes())
            .await
            .expect("Unable to write data to file");
        buf_writer
            .write_all(b"]")
            .await
            .expect("Unable to write data to file");
    } else {
        // Move the cursor back to overwrite the last "]" character
        buf_writer
            .seek(SeekFrom::End(-1))
            .await
            .expect("Unable to seek in file");

        buf_writer
            .write_all(b",")
            .await
            .expect("Unable to write data to file");

        buf_writer
            .write_all(json_string.as_bytes())
            .await
            .expect("Unable to write data to file");

        buf_writer
            .write_all(b"]")
            .await
            .expect("Unable to write data to file");
    }

    // Flush and sync the buffer to ensure all data is written to disk
    buf_writer.flush().await.expect("Failed to flush data");
    file.sync_all().await.expect("Failed to sync file");

    Ok(())
}

/// Writes a list of `JobInfo` objects to a JSON file at the specified path.
///
/// # Arguments
/// * `jobs` - A vector of `JobInfo` objects to be serialized and written to the file.
/// * `path` - The filesystem path where the JSON file will be written.
///
/// # Examples
/// ```
/// use std::path::Path;
/// use datafusion::common::Result;
///
/// async fn example_usage() -> Result<()> {
///     let jobs = ...
///     let path = Path::new("/path/to/jobs.json");
///     write_jobs_to_json(jobs, path).await?;
///     Ok(())
/// }
/// ```
pub async fn write_jobs_to_json(jobs: Vec<JobInfo>, path: &Path) -> Result<(), serde_json::Error> {
    let json_string = serde_json::to_string_pretty(&jobs)?;
    let mut file = File::create(path).await.expect("Unable to create file");
    file.write_all(json_string.as_bytes())
        .await
        .expect("Unable to write data to file");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::frontend::JobInfo;
    use crate::profiling::{append_job_to_json_file, write_jobs_to_json};
    use crate::server::composable_database::QueryStatus;
    use chrono::{TimeZone, Utc};
    use std::path::Path;
    use tokio::fs;
    use tokio::fs::File;

    fn example_job_info() -> JobInfo {
        return JobInfo {
            query_id: 0,
            sql_string: String::from("SELECT * FROM RANDOM_TABLE"),
            status: QueryStatus::Done,
            submitted_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            finished_at: Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
            result: None,
        };
    }

    #[tokio::test]
    async fn test_append_job_to_json_file() {
        let example_job_info = example_job_info();
        let path = "./tmp_append.json";
        let _ = fs::remove_file(path).await;
        append_job_to_json_file(&example_job_info, Path::new(path))
            .await
            .expect("test_append_job_to_json_file: fail to write job summary");
        assert!(
            File::open(path).await.is_ok(),
            "File does not exist after writing"
        );
        append_job_to_json_file(&example_job_info, Path::new(path))
            .await
            .expect("test_append_job_to_json_file: fail to write job summary");
        assert!(
            File::open(path).await.is_ok(),
            "File does not exist after writing"
        );
        fs::remove_file(path)
            .await
            .expect("Failed to delete test file after testing");
    }

    #[tokio::test]
    async fn test_write_job_to_json() {
        let example_job_info = example_job_info();
        let jobs = vec![example_job_info.clone(), example_job_info.clone()];
        let path = "./tmp_write.json";
        let _ = fs::remove_file(path).await;
        write_jobs_to_json(jobs, Path::new(path))
            .await
            .expect("test_append_job_to_json_file: fail to write job summary");
        assert!(
            File::open(path).await.is_ok(),
            "File does not exist after writing"
        );
        fs::remove_file(path)
            .await
            .expect("Failed to delete test file after testing");
    }
}
