

/*

Steps for integration testing:
1. Start the scheduler
2. Start th





*/

use serde::Deserialize;
use tonic::transport::Channel;
use composable_database::scheduler_api_client::SchedulerApiClient;
use composable_database::{ScheduleQueryArgs, NotifyTaskStateArgs};
use lazy_static::lazy_static;
use composable_database::TaskId;



lazy_static! {
    static HANDSHAKE_QUERY_ID = -1li64 as u64;
    static HANDSHAKE_TASK_ID = -1li64 as u64;
    static ref HANDSHAKE_TASK_ID: TaskId = TaskId {
        query_id: HANDSHAKE_QUERY_ID,
        task_id: HANDSHAKE_TASK_ID,
    };
}


// Format definitions for the config file
#[derive(Deserialize)]
struct Config {
    scheduler: Vec<SchedulerConfig>,
    executors: Vec<ExecutorConfig>,
}

#[derive(Deserialize)]
struct SchedulerConfig {
    ip_addr: String,
    port: u16,
}

#[derive(Deserialize)]
struct ExecutorConfig {
    id: u32,
    ip_addr: String,
    port: u16,
    numa_node: u32,
}


fn read_config() -> Config {
    let config_str = fs::read_to_string("config.toml").expect("Failed to read config file");
    toml::from_str(&config_str).expect("Failed to parse config file")
}

// Starts the scheduler gRPC service
async fn start_scheduler_server(addr: String) {
    let addr = addr.parse().expect("Invalid address");
    let scheduler = MyScheduler::default();

    println!("Scheduler listening on {}", addr);

    if let Err(e) = Server::builder()
        .add_service(SchedulerApiServer::new(scheduler))
        .serve(addr)
        .await
    {
        eprintln!("Server error: {}", e);
    }
}

// Starts the executor gRPC service
async fn start_executor_client(executor: ExecutorConfig, scheduler_addr: String) {
    println!("Executor {} connecting to scheduler at {}", executor.id, scheduler_addr);

    // Create a connection to the scheduler
    let channel = Channel::from_shared(scheduler_addr)
        .expect("Invalid scheduler address")
        .connect()
        .await
        .expect("Failed to connect to scheduler");

    // Create a client using the channel
    let mut client = SchedulerApiClient::new(channel);

    // Send initial request with handshake task ID
    let handshake = tonic::Request::new(NotifyTaskStateArgs {
        task: Some(HANDSHAKE_TASK_ID),
        success: true,
        result: Vec::new(),
    });

    let mut task_id = composable_database::TaskId {
        query_id: HANDSHAKE_QUERY_ID,
        task_id: HANDSHAKE_TASK_ID,
    };

    loop {
        let request = if task_id.query_id == HANDSHAKE_QUERY_ID && task_id.task_id == HANDSHAKE_TASK_ID {
            initial_request.clone()
        } else {
            tonic::Request::new(NotifyTaskStateArgs {
                task: Some(task_id.clone()),
                success: true,
                result: Vec::new(),
            })
        };

        match client.notify_task_state(request).await {
            Ok(response) => {
                let response_inner = response.into_inner();
                if response_inner.has_new_task {
                    task_id = response_inner.task.unwrap_or_default();

                    // TODO: execute the physical plan
                    // let new_result 
                } else {
                    // No new task available
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to send task state: {}", e);
                break;
            }
        }
    }
}

// TODO: add a function to run a sql query on a single executor


// TODO: research function to compare equality of two results (apache arrow?)



fn generate_refsol() {
    
}

// Make this into a command line app
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let config = read_config();

    // Start the scheduler server
    let scheduler_addr = format!("{}:{}", config.scheduler[0].ip_addr, config.scheduler[0].port);
    tokio::spawn(async move {
        start_scheduler_server(scheduler_addr).await;
    });

   // Start executor clients
   for executor in config.executors {
    let scheduler_addr_clone = scheduler_addr.clone();
    tokio::spawn(async move {
        start_executor_client(executor, scheduler_addr_clone).await;
    });
    }

    // start an reference executor instance to verify correctness
    let reference_executor = DatafusionExecutor::new();

    // get all the execution plans and pre-compute all the reference results
    let execution_plans = get_execution_plan_from_file(file_path_str).await.expect("Failed to get execution plans");
    let mut results = Vec::new();
    for plan in execution_plans {
        match executor.execute_plan(plan).await {
            Ok(dataframe) => {
                results.push(dataframe);
            },
            Err(e) => eprintln!("Failed to execute plan: {}", e),
        }
    }
    
    // TODO: make this a command line program where it runs a file, verify files by comparing recordbatches


    let test_files = list_all_slt_files("./test_files");

    for file_path in test_files {
        let file_path_str = file_path
            .to_str()
            .expect("Failed to convert path to string");

        eprintln!("Processing test file: {}", file_path_str);

        match get_execution_plan_from_file(file_path_str).await {
            Ok(plans) => {
                

                
                



            }
            Err(e) => {
                eprintln!(
                    "Failed to get execution plans from file {}: {}",
                    file_path_str, e
                );
                panic!("Test failed due to error with file: {}", file_path_str);
            }
        }
    }












    


    

    Ok(())
}