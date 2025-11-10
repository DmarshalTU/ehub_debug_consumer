use anyhow::{Context, Result};
use azure_identity::ClientSecretCredential;
use azure_messaging_eventhubs::{
    ConsumerClient, OpenReceiverOptions, StartLocation, StartPosition,
};
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct EventStats {
    partition: String,
    consumer_group: String,
    total_events: u64,
    last_event_time: Option<DateTime<Utc>>,
    last_enqueued_time: Option<DateTime<Utc>>,
    max_latency_ms: u64,
    min_latency_ms: Option<u64>,
    total_latency_ms: u128,
    errors: u64,
    start_time: Instant,
}

impl EventStats {
    fn new(partition: String, consumer_group: String) -> Self {
        Self {
            partition,
            consumer_group,
            total_events: 0,
            last_event_time: None,
            last_enqueued_time: None,
            max_latency_ms: 0,
            min_latency_ms: None,
            total_latency_ms: 0,
            errors: 0,
            start_time: Instant::now(),
        }
    }

    fn record_event(&mut self, latency_ms: u64, enqueued_time: Option<DateTime<Utc>>) {
        self.total_events += 1;
        self.last_event_time = Some(Utc::now());
        self.last_enqueued_time = enqueued_time;
        
        if latency_ms > self.max_latency_ms {
            self.max_latency_ms = latency_ms;
        }
        
        if let Some(min) = self.min_latency_ms {
            if latency_ms < min {
                self.min_latency_ms = Some(latency_ms);
            }
        } else {
            self.min_latency_ms = Some(latency_ms);
        }
        
        self.total_latency_ms += latency_ms as u128;
    }

    fn record_error(&mut self) {
        self.errors += 1;
    }

    fn average_latency_ms(&self) -> u64 {
        if self.total_events > 0 {
            (self.total_latency_ms / self.total_events as u128) as u64
        } else {
            0
        }
    }

    fn events_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_events as f64 / elapsed
        } else {
            0.0
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with human-readable format
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,azure_messaging_eventhubs=trace,azure_core=trace".to_string())
        )
        .with_ansi(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("==========================================");
    info!("Azure Event Hubs Debug Consumer Starting");
    info!("==========================================");

    // Read configuration from environment variables
    let host: String = env::var("EVENTHUBS_HOST")
        .context("EVENTHUBS_HOST environment variable is required")?;
    let eventhub: String = env::var("EVENTHUB_NAME")
        .context("EVENTHUB_NAME environment variable is required")?;
    
    let consumer_groups_str = env::var("CONSUMER_GROUPS")
        .unwrap_or_else(|_| "default".to_string());
    let consumer_groups: Vec<String> = consumer_groups_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if consumer_groups.is_empty() {
        anyhow::bail!("At least one consumer group must be specified in CONSUMER_GROUPS");
    }

    info!("Configuration:");
    info!("  Event Hub Host: {}", host);
    info!("  Event Hub Name: {}", eventhub);
    info!("  Consumer Groups: {:?}", consumer_groups);
    
    // Log authentication method
    let auth_method = if env::var("AZURE_CLIENT_ID").is_ok() 
        && env::var("AZURE_CLIENT_SECRET").is_ok() 
        && env::var("AZURE_TENANT_ID").is_ok() {
        "Service Principal (from environment variables)"
    } else {
        "DefaultAzureCredential (Managed Identity, Workload Identity, or Azure CLI)"
    };
    info!("  Authentication: {}", auth_method);
    info!("");

    info!("✓ Ready to create credentials");
    info!("");

    // Statistics tracking
    let stats: Arc<Mutex<HashMap<String, EventStats>>> = Arc::new(Mutex::new(HashMap::new()));

    // Spawn tasks for each consumer group
    let mut handles = Vec::new();

    for consumer_group in consumer_groups {
        let host_clone = host.clone();
        let eventhub_clone = eventhub.clone();
        let stats_clone = stats.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = consume_from_group(
                host_clone,
                eventhub_clone,
                &consumer_group,
                stats_clone,
            ).await {
                error!("Consumer group '{}' failed: {:?}", consumer_group, e);
            }
        });

        handles.push(handle);
    }

    // Spawn statistics reporter
    let stats_reporter = stats.clone();
    let reporter_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let stats_guard = stats_reporter.lock().await;
            if !stats_guard.is_empty() {
                info!("");
                info!("========== Statistics Summary (last 30s) ==========");
                for (_key, stat) in stats_guard.iter() {
                    info!("Consumer Group: {}, Partition: {}", stat.consumer_group, stat.partition);
                    info!("  Total Events: {}", stat.total_events);
                    info!("  Events/sec: {:.2}", stat.events_per_second());
                    info!("  Average Latency: {} ms", stat.average_latency_ms());
                    info!("  Min Latency: {} ms", stat.min_latency_ms.unwrap_or(0));
                    info!("  Max Latency: {} ms", stat.max_latency_ms);
                    if let Some(last_time) = stat.last_event_time {
                        info!("  Last Event Time: {}", last_time.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                    }
                    if let Some(enq_time) = stat.last_enqueued_time {
                        info!("  Last Enqueued Time: {}", enq_time.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                    }
                    if stat.errors > 0 {
                        warn!("  Errors: {}", stat.errors);
                    }
                    info!("");
                }
                info!("==================================================");
                info!("");
            }
        }
    });

    // Wait for all consumer tasks
    for handle in handles {
        if let Err(e) = handle.await {
            error!("Consumer task panicked: {:?}", e);
        }
    }

    // Cancel reporter
    reporter_handle.abort();

    Ok(())
}

/// Create Azure credential based on environment variables
/// Supports:
/// 1. Service Principal (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID) - for Kubernetes
/// 2. DeveloperToolsCredential (for local development with az login)
/// 
/// Note: For Managed Identity in AKS, use Workload Identity which requires additional K8s configuration.
/// The Service Principal approach is simpler and recommended for most use cases.
fn create_credential() -> Result<Arc<ClientSecretCredential>> {
    // Check if Service Principal credentials are provided (required for Kubernetes)
    if let (Ok(client_id), Ok(client_secret), Ok(tenant_id)) = (
        env::var("AZURE_CLIENT_ID"),
        env::var("AZURE_CLIENT_SECRET"),
        env::var("AZURE_TENANT_ID"),
    ) {
        info!("Using Service Principal authentication (suitable for Kubernetes)");
        let credential = ClientSecretCredential::new(
            &tenant_id,
            client_id,
            client_secret.into(),
            None,
        )
        .context("Failed to create ClientSecretCredential")?;
        Ok(credential)
    } else {
        // Fall back to DeveloperToolsCredential for local development only
        // This will NOT work in Kubernetes pods
        warn!("Service Principal credentials not found. Using DeveloperToolsCredential (local dev only).");
        warn!("⚠️  For Kubernetes: You MUST set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID");
        
        // For local dev, we can't return ClientSecretCredential, so we need a different approach
        // Actually, let's just require Service Principal for now and make it clear
        anyhow::bail!(
            "Service Principal credentials required for Kubernetes deployment.\n\
            Set environment variables: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID\n\
            For local development, you can use 'az login' but this won't work in pods."
        );
    }
}

async fn consume_from_group(
    host: String,
    eventhub: String,
    consumer_group: &str,
    stats: Arc<Mutex<HashMap<String, EventStats>>>,
) -> Result<()> {
    info!("[{}] Creating credential...", consumer_group);
    let credential = create_credential()
        .context("Failed to create credential")?;
    
    info!("[{}] Creating consumer client...", consumer_group);
    let _consumer = ConsumerClient::builder()
        .open(host.as_str(), eventhub.clone(), credential)
        .await
        .context("Failed to open consumer client")?;

    info!("[{}] ✓ Consumer client opened successfully", consumer_group);

    // Get partition IDs - we'll try to discover them by attempting to open receivers
    // For now, we'll try partitions 0-31 (common range)
    info!("[{}] Discovering partitions...", consumer_group);
    
    let mut partition_tasks = Vec::new();
    
    // Try to open receivers for partitions 0-31
    for partition_id in 0..32 {
        let partition_str = partition_id.to_string();
        let host_clone = host.to_string();
        let eventhub_clone = eventhub.to_string();
        let consumer_group_clone = consumer_group.to_string();
        let stats_clone = stats.clone();

        let task = tokio::spawn(async move {
            // Create a new credential for this partition
            let partition_credential = match create_credential() {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "[{}] Failed to create credential for partition {}: {:?}",
                        consumer_group_clone, partition_str, e
                    );
                    return;
                }
            };
            
            // Create a new consumer client for this partition/consumer group combination
            let partition_consumer = match ConsumerClient::builder()
                .open(host_clone.as_str(), eventhub_clone.clone(), partition_credential)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "[{}] Failed to create consumer for partition {}: {:?}",
                        consumer_group_clone, partition_str, e
                    );
                    return;
                }
            };

            match partition_consumer
                .open_receiver_on_partition(
                    partition_str.clone(),
                    Some(OpenReceiverOptions {
                        start_position: Some(StartPosition {
                            location: StartLocation::Earliest,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                )
                .await
            {
                Ok(receiver) => {
                    info!(
                        "[{}] ✓ Successfully opened receiver for partition {}",
                        consumer_group_clone, partition_str
                    );
                    
                    let stats_key = format!("{}:{}", consumer_group_clone, partition_str);
                    let mut stats_guard = stats_clone.lock().await;
                    stats_guard.insert(
                        stats_key.clone(),
                        EventStats::new(partition_str.clone(), consumer_group_clone.clone()),
                    );
                    drop(stats_guard);

                    // Clone values for error handling
                    let consumer_group_for_error = consumer_group_clone.clone();
                    let partition_str_for_error = partition_str.clone();
                    let stats_key_for_error = stats_key.clone();
                    let stats_clone_for_error = stats_clone.clone();

                    if let Err(e) = consume_from_partition(
                        receiver,
                        partition_str,
                        consumer_group_clone,
                        stats_clone,
                        stats_key,
                    )
                    .await
                    {
                        error!(
                            "[{}] Partition {} consumer error: {:?}",
                            consumer_group_for_error, partition_str_for_error, e
                        );
                        let mut stats_guard = stats_clone_for_error.lock().await;
                        if let Some(stat) = stats_guard.get_mut(&stats_key_for_error) {
                            stat.record_error();
                        }
                    }
                }
                Err(e) => {
                    // Partition might not exist, which is fine
                    warn!(
                        "[{}] Could not open receiver for partition {}: {:?}",
                        consumer_group_clone, partition_str, e
                    );
                }
            }
        });

        partition_tasks.push(task);
    }

    // Wait for all partition consumers
    for task in partition_tasks {
        if let Err(e) = task.await {
            error!("Partition consumer task panicked: {:?}", e);
        }
    }

    Ok(())
}

async fn consume_from_partition(
    receiver: azure_messaging_eventhubs::EventReceiver,
    partition: String,
    consumer_group: String,
    stats: Arc<Mutex<HashMap<String, EventStats>>>,
    stats_key: String,
) -> Result<()> {
    info!(
        "[{}:{}] Starting to consume events from partition",
        consumer_group, partition
    );

    let mut event_stream = receiver.stream_events();
    let mut last_log_time = Instant::now();
    let log_interval = Duration::from_secs(60);

    while let Some(event_result) = event_stream.next().await {
        let _receive_time = Instant::now();
        let receive_timestamp = Utc::now();

        match event_result {
            Ok(event) => {
                // Extract all possible event information
                let sequence_number = event.sequence_number();
                let offset = event.offset();
                let enqueued_time = event.enqueued_time();
                let partition_key = event.partition_key();
                let system_properties = event.system_properties();
                let event_data = event.event_data();
                let body = event_data.body();
                let body_size = body.as_ref().map(|b| b.len()).unwrap_or(0);

                // Convert SystemTime to DateTime<Utc> for latency calculation
                let enqueued_datetime = enqueued_time.map(|st| {
                    let duration_since_epoch = st.duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    DateTime::<Utc>::from_timestamp(
                        duration_since_epoch.as_secs() as i64,
                        duration_since_epoch.subsec_nanos(),
                    ).unwrap_or_else(|| Utc::now())
                });

                // Calculate latency if enqueued_time is available
                let latency_ms = if let Some(enq_dt) = enqueued_datetime {
                    let latency = receive_timestamp.signed_duration_since(enq_dt);
                    latency.num_milliseconds().max(0) as u64
                } else {
                    0
                };

                // Update statistics
                {
                    let mut stats_guard = stats.lock().await;
                    if let Some(stat) = stats_guard.get_mut(&stats_key) {
                        stat.record_event(latency_ms, enqueued_datetime);
                    }
                }

                // Log detailed event information in human-readable format
                info!("");
                info!("╔═══════════════════════════════════════════════════════════════════════════════╗");
                info!("║ EVENT RECEIVED                                                                 ║");
                info!("╠═══════════════════════════════════════════════════════════════════════════════╣");
                info!("║ Consumer Group: {:<65} ║", consumer_group);
                info!("║ Partition:      {:<65} ║", partition);
                info!("║ Sequence Number: {:<64} ║", sequence_number.map(|s| s.to_string()).unwrap_or_else(|| "N/A".to_string()));
                info!("║ Offset:         {:<65} ║", offset.as_ref().map(|s| s.as_str()).unwrap_or("N/A"));
                
                if let Some(enq_dt) = enqueued_datetime {
                    info!("║ Enqueued Time:  {:<65} ║", enq_dt.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                } else {
                    info!("║ Enqueued Time:  {:<65} ║", "N/A");
                }
                
                info!("║ Received Time:  {:<65} ║", receive_timestamp.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                
                if latency_ms > 0 {
                    let latency_color = if latency_ms > 5000 {
                        "⚠️  HIGH"
                    } else if latency_ms > 1000 {
                        "⚠️  MEDIUM"
                    } else {
                        "✓ OK"
                    };
                    info!("║ Latency:        {:<65} ║", format!("{} ms ({})", latency_ms, latency_color));
                } else {
                    info!("║ Latency:        {:<65} ║", "N/A");
                }
                
                if let Some(pkey) = partition_key {
                    info!("║ Partition Key:  {:<65} ║", pkey);
                } else {
                    info!("║ Partition Key:  {:<65} ║", "None");
                }
                
                info!("║ Body Size:      {:<65} ║", format!("{} bytes", body_size));
                
                if !system_properties.is_empty() {
                    info!("║ System Properties:                                                          ║");
                    for (key, value) in system_properties.iter() {
                        let value_str = format!("{:?}", value);
                        let display_value = if value_str.len() > 60 {
                            format!("{}...", &value_str[..57])
                        } else {
                            value_str
                        };
                        info!("║   {}: {:<63} ║", key, display_value);
                    }
                } else {
                    info!("║ System Properties: None                                                      ║");
                }
                
                // Try to decode body as UTF-8 for display
                if let Some(body_bytes) = body {
                    match std::str::from_utf8(body_bytes) {
                        Ok(body_str) => {
                            let display_body = if body_str.len() > 200 {
                                format!("{}...", &body_str[..197])
                            } else {
                                body_str.to_string()
                            };
                            info!("║ Body (UTF-8):                                                                 ║");
                            for line in display_body.lines() {
                                let display_line = if line.len() > 75 {
                                    format!("{}...", &line[..72])
                                } else {
                                    line.to_string()
                                };
                                info!("║   {:<75} ║", display_line);
                            }
                        }
                        Err(_) => {
                            info!("║ Body (Hex):                                                                   ║");
                            let hex_preview: String = body_bytes.iter().take(100).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
                            info!("║   {:<75} ║", hex_preview);
                        }
                    }
                } else {
                    info!("║ Body:           Empty                                                          ║");
                }
                
                info!("╚═══════════════════════════════════════════════════════════════════════════════╝");
                info!("");

                // Periodic summary log
                if last_log_time.elapsed() >= log_interval {
                    let stats_guard = stats.lock().await;
                    if let Some(stat) = stats_guard.get(&stats_key) {
                        info!(
                            "[{}:{}] Summary - Events: {}, Rate: {:.2}/s, Avg Latency: {}ms, Max Latency: {}ms",
                            consumer_group,
                            partition,
                            stat.total_events,
                            stat.events_per_second(),
                            stat.average_latency_ms(),
                            stat.max_latency_ms
                        );
                    }
                    last_log_time = Instant::now();
                }
            }
            Err(err) => {
                error!(
                    "[{}:{}] ❌ ERROR receiving event: {:?}",
                    consumer_group, partition, err
                );
                
                // Update error statistics
                {
                    let mut stats_guard = stats.lock().await;
                    if let Some(stat) = stats_guard.get_mut(&stats_key) {
                        stat.record_error();
                    }
                }

                // Log full error details
                error!("Error Details:");
                error!("  Consumer Group: {}", consumer_group);
                error!("  Partition: {}", partition);
                error!("  Timestamp: {}", receive_timestamp.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                error!("  Error: {:?}", err);
                error!("");
            }
        }
    }

    warn!(
        "[{}:{}] Event stream ended",
        consumer_group, partition
    );

    Ok(())
}
