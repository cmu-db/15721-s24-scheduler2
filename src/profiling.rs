use crate::frontend::JobInfo;
use datafusion::common::Result;
use serde_json;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

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
        .append(true)
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
    } else {
        buf_writer
            .write_all(b",")
            .await
            .expect("Unable to write data to file");
    }

    buf_writer
        .write_all(json_string.as_bytes())
        .await
        .expect("Unable to write data to file");
    buf_writer
        .write_all(b"]")
        .await
        .expect("Unable to write data to file");

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
