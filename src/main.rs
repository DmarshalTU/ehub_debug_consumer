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

// ABSOLUTE HARD LIMIT - never scan beyond partition 7
const MAX_PARTITION_LIMIT: u32 = 7;

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
async fn main() {
    // Print to stderr immediately (Kubernetes captures this even if logging fails)
    eprintln!("[ehub-debug-consumer] Starting application...");
    eprintln!("[ehub-debug-consumer] Version: {}", env!("CARGO_PKG_VERSION"));
    
    // Initialize tracing with human-readable format
    // Default to INFO level to reduce noise, but allow override via RUST_LOG
    let default_log = "info,azure_messaging_eventhubs=warn,azure_core=warn,azure_core_amqp=warn".to_string();
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or(default_log)
        )
        .with_ansi(true)
        .with_target(false)  // Hide target to reduce noise
        .with_thread_ids(false)  // Hide thread IDs to reduce noise
        .with_thread_names(false)  // Hide thread names to reduce noise
        .with_file(false)  // Hide file paths to reduce noise
        .with_line_number(false)  // Hide line numbers to reduce noise
        .with_writer(std::io::stderr)  // Write to stderr so Kubernetes captures it
        .init();

    eprintln!("[ehub-debug-consumer] Logging initialized");

    // Run the main logic and handle errors
    if let Err(e) = run().await {
        eprintln!("[ehub-debug-consumer] FATAL ERROR: {:?}", e);
        error!("==========================================");
        error!("FATAL ERROR - Application is exiting");
        error!("==========================================");
        error!("Error: {:?}", e);
        error!("");
        error!("This usually means:");
        error!("  1. Missing required environment variables (EVENTHUBS_HOST, EVENTHUB_NAME, etc.)");
        error!("  2. Authentication failure (check AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)");
        error!("  3. Invalid configuration");
        error!("");
        error!("Check your Kubernetes deployment configuration and environment variables.");
        error!("==========================================");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {

    info!("==========================================");
    info!("Azure Event Hubs Debug Consumer Starting");
    info!("==========================================");

    // Read configuration from environment variables
    // Check for connection string first - it contains the host
    // Note: Helm sets empty strings as env vars, so we need to check for empty strings too
    let (host, _connection_string_parsed) = if let Ok(conn_str) = env::var("EVENTHUB_CONNECTION_STRING") {
        if conn_str.trim().is_empty() {
            anyhow::bail!("EVENTHUB_CONNECTION_STRING is set but empty. Please provide a valid connection string or set EVENTHUBS_HOST instead.");
        }
        let (parsed_host, _, _) = parse_connection_string(&conn_str)
            .context("Failed to parse EVENTHUB_CONNECTION_STRING")?;
        (parsed_host, Some(conn_str))
    } else {
        let host_env = env::var("EVENTHUBS_HOST")
            .context("EVENTHUBS_HOST or EVENTHUB_CONNECTION_STRING environment variable is required")?;
        if host_env.trim().is_empty() {
            anyhow::bail!("EVENTHUBS_HOST is set but empty. Please provide a valid Event Hub host (e.g., namespace.servicebus.windows.net)");
        }
        (host_env, None)
    };
    
    let eventhub: String = env::var("EVENTHUB_NAME")
        .context("EVENTHUB_NAME environment variable is required")?;
    if eventhub.trim().is_empty() {
        anyhow::bail!("EVENTHUB_NAME is set but empty. Please provide a valid Event Hub name.");
    }
    
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

    // Get partition range configuration
    let max_partition_env = env::var("MAX_PARTITION");
    let max_partition_raw = max_partition_env
        .as_ref()
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(7);  // Default to 7 (most Event Hubs have 1-8 partitions)
    
    // HARD CAP at MAX_PARTITION_LIMIT to prevent scanning non-existent partitions
    let max_partition = if max_partition_raw > MAX_PARTITION_LIMIT {
        eprintln!("[ehub-debug-consumer] WARNING: MAX_PARTITION was {} but capping at {} to prevent errors!", max_partition_raw, MAX_PARTITION_LIMIT);
        warn!("MAX_PARTITION was {} but capping at {} to prevent scanning non-existent partitions", max_partition_raw, MAX_PARTITION_LIMIT);
        MAX_PARTITION_LIMIT
    } else {
        max_partition_raw
    };
    
    let min_partition_env = env::var("MIN_PARTITION");
    let min_partition = min_partition_env
        .as_ref()
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    
    info!("Configuration:");
    info!("  Event Hub Host: {}", host);
    info!("  Event Hub Name: {}", eventhub);
    info!("  Consumer Groups: {:?}", consumer_groups);
    
    // Log what we actually read from environment - MAKE THIS VERY VISIBLE
    eprintln!("[ehub-debug-consumer] MAX_PARTITION check:");
    if let Ok(val) = max_partition_env {
        eprintln!("[ehub-debug-consumer] MAX_PARTITION env var: '{}' (parsed as: {})", val, max_partition);
        info!("  MAX_PARTITION env var: '{}' (parsed as: {})", val, max_partition);
    } else {
        eprintln!("[ehub-debug-consumer] MAX_PARTITION env var: NOT SET (using default: {})", max_partition);
        info!("  MAX_PARTITION env var: not set (using default: {})", max_partition);
    }
    if let Ok(val) = min_partition_env {
        info!("  MIN_PARTITION env var: '{}' (parsed as: {})", val, min_partition);
    } else {
        info!("  MIN_PARTITION env var: not set (using default: {})", min_partition);
    }
    
    eprintln!("[ehub-debug-consumer] Will scan partitions {}-{}", min_partition, max_partition);
    info!("  ═══════════════════════════════════════════════════════════════════════════════");
    info!("  Partition Range: {}-{} (will scan partitions {}-{})", min_partition, max_partition, min_partition, max_partition);
    info!("  ═══════════════════════════════════════════════════════════════════════════════");
    
    if max_partition > MAX_PARTITION_LIMIT {
        eprintln!("[ehub-debug-consumer] WARNING: MAX_PARTITION is {} (should be {} for partitions 0-{})!", max_partition, MAX_PARTITION_LIMIT, MAX_PARTITION_LIMIT);
        warn!("  ⚠️  MAX_PARTITION is set to {} - this may try non-existent partitions!", max_partition);
        warn!("     Most Event Hubs have 1-8 partitions. Set MAX_PARTITION={} if you have 8 partitions (0-{})", MAX_PARTITION_LIMIT, MAX_PARTITION_LIMIT);
    }
    
    // Log authentication method
    let auth_method = if env::var("EVENTHUB_CONNECTION_STRING").is_ok() {
        "Shared Access Policy (Connection String)"
    } else if env::var("AZURE_CLIENT_ID").is_ok() 
        && env::var("AZURE_CLIENT_SECRET").is_ok() 
        && env::var("AZURE_TENANT_ID").is_ok() {
        "Service Principal (from environment variables)"
    } else {
        "Not configured - will fail"
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
        // ENFORCE HARD LIMIT before passing to function
        let min_partition_clone = min_partition;
        let max_partition_clone = if max_partition > MAX_PARTITION_LIMIT {
            eprintln!("[ehub-debug-consumer] CRITICAL: max_partition is {} but capping to {} before spawning task!", max_partition, MAX_PARTITION_LIMIT);
            error!("CRITICAL: max_partition is {} but capping to {} before spawning task!", max_partition, MAX_PARTITION_LIMIT);
            MAX_PARTITION_LIMIT
        } else {
            max_partition
        };

        eprintln!("[ehub-debug-consumer] Spawning task for consumer group '{}' with partition range {}-{}", consumer_group, min_partition_clone, max_partition_clone);
        let handle = tokio::spawn(async move {
            if let Err(e) = consume_from_group(
                host_clone,
                eventhub_clone,
                &consumer_group,
                stats_clone,
                min_partition_clone,
                max_partition_clone,
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

/// Parse Event Hub connection string
/// Format: Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy-name;SharedAccessKey=key
fn parse_connection_string(conn_str: &str) -> Result<(String, String, String)> {
    if conn_str.is_empty() {
        anyhow::bail!("Connection string is empty");
    }
    
    let mut endpoint = None;
    let mut key_name = None;
    let mut key = None;
    
    for part in conn_str.split(';') {
        if let Some((k, v)) = part.split_once('=') {
            match k.trim() {
                "Endpoint" => {
                    // Extract host from endpoint: sb://namespace.servicebus.windows.net/
                    // Handle both with and without trailing slash
                    let host = v
                        .trim()
                        .strip_prefix("sb://")
                        .ok_or_else(|| anyhow::anyhow!("Invalid Endpoint format: must start with 'sb://'"))?
                        .trim_end_matches('/');
                    
                    if host.is_empty() {
                        anyhow::bail!("Invalid Endpoint format: host is empty");
                    }
                    
                    endpoint = Some(host.to_string());
                }
                "SharedAccessKeyName" => {
                    if !v.trim().is_empty() {
                        key_name = Some(v.trim().to_string());
                    }
                }
                "SharedAccessKey" => {
                    if !v.trim().is_empty() {
                        key = Some(v.trim().to_string());
                    }
                }
                _ => {}
            }
        }
    }
    
    Ok((
        endpoint.context("Missing Endpoint in connection string. Format: Endpoint=sb://namespace.servicebus.windows.net/;...")?,
        key_name.context("Missing SharedAccessKeyName in connection string")?,
        key.context("Missing SharedAccessKey in connection string")?,
    ))
}

/// Create Azure credential based on environment variables
/// Supports (in order of preference):
/// 1. Shared Access Policy (EVENTHUB_CONNECTION_STRING) - simplest, recommended for Event Hubs
/// 2. Service Principal (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID) - for advanced scenarios
/// 
/// Connection string format: Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy-name;SharedAccessKey=key
/// 
/// Note: The SDK requires TokenCredential, so for connection strings we still need Service Principal.
/// However, the connection string is used to extract the host automatically.
/// For true SAS-only authentication, the SDK would need to support it natively.
fn create_credential() -> Result<Arc<ClientSecretCredential>> {
    // Check if Service Principal credentials are provided
    if let (Ok(client_id), Ok(client_secret), Ok(tenant_id)) = (
        env::var("AZURE_CLIENT_ID"),
        env::var("AZURE_CLIENT_SECRET"),
        env::var("AZURE_TENANT_ID"),
    ) {
        if env::var("EVENTHUB_CONNECTION_STRING").is_ok() {
            info!("Using Shared Access Policy (connection string) + Service Principal authentication");
        } else {
            info!("Using Service Principal authentication");
        }
        let credential = ClientSecretCredential::new(
            &tenant_id,
            client_id,
            client_secret.into(),
            None,
        )
        .context("Failed to create ClientSecretCredential")?;
        Ok(credential)
    } else if env::var("EVENTHUB_CONNECTION_STRING").is_ok() {
        // Connection string provided but no Service Principal
        // The connection string is used for host extraction, but we still need SP for auth
        anyhow::bail!(
            "Connection string provided but Service Principal credentials are required.\n\
            The SDK requires TokenCredential for authentication.\n\
            Please also set: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID\n\
            \n\
            Note: The connection string will be used to automatically extract the host."
        );
    } else {
        anyhow::bail!(
            "Authentication required. Set one of:\n\
            1. EVENTHUB_CONNECTION_STRING (for host extraction) + Service Principal (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)\n\
            2. AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID (Service Principal only)"
        );
    }
}

async fn consume_from_group(
    host: String,
    eventhub: String,
    consumer_group: &str,
    stats: Arc<Mutex<HashMap<String, EventStats>>>,
    min_partition: u32,
    max_partition: u32,
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
    
    eprintln!("[ehub-debug-consumer] [{}] Discovering partitions (range: {}-{})...", consumer_group, min_partition, max_partition);
    info!("[{}] Discovering partitions (range: {}-{})...", consumer_group, min_partition, max_partition);
    
    // ENFORCE HARD LIMIT - never scan beyond MAX_PARTITION_LIMIT
    let effective_max = if max_partition > MAX_PARTITION_LIMIT {
        eprintln!("[ehub-debug-consumer] [{}] WARNING: max_partition is {} but enforcing limit of {}!", consumer_group, max_partition, MAX_PARTITION_LIMIT);
        warn!("[{}] max_partition is {} but enforcing limit of {} to prevent errors", consumer_group, max_partition, MAX_PARTITION_LIMIT);
        MAX_PARTITION_LIMIT
    } else {
        max_partition
    };
    
    let mut partition_tasks = Vec::new();
    
    // ABSOLUTE HARD LIMIT - never exceed MAX_PARTITION_LIMIT
    let absolute_max = effective_max.min(MAX_PARTITION_LIMIT);
    if absolute_max != effective_max {
        eprintln!("[ehub-debug-consumer] [{}] CRITICAL: effective_max was {} but forcing to {}!", consumer_group, effective_max, MAX_PARTITION_LIMIT);
        error!("[{}] CRITICAL: effective_max was {} but forcing to {}!", consumer_group, effective_max, MAX_PARTITION_LIMIT);
    }
    
    // FINAL CHECK: absolute_max MUST be <= MAX_PARTITION_LIMIT
    assert!(absolute_max <= MAX_PARTITION_LIMIT, "absolute_max ({}) must be <= MAX_PARTITION_LIMIT ({})", absolute_max, MAX_PARTITION_LIMIT);
    
    eprintln!("[ehub-debug-consumer] [{}] Will scan partitions {}-{} (HARD LIMIT: {})", consumer_group, min_partition, absolute_max, MAX_PARTITION_LIMIT);
    eprintln!("[ehub-debug-consumer] [{}] Loop will iterate: {}..={} (inclusive range)", consumer_group, min_partition, absolute_max);
    info!("[{}] Will scan partitions {}-{} (HARD LIMIT: {})", consumer_group, min_partition, absolute_max, MAX_PARTITION_LIMIT);
    
    // Build the partition list explicitly to avoid any range issues
    let mut partition_list: Vec<u32> = Vec::new();
    for pid in min_partition..=absolute_max {
        if pid <= MAX_PARTITION_LIMIT {
            partition_list.push(pid);
        }
    }
    
    eprintln!("[ehub-debug-consumer] [{}] Partition list: {:?} (total: {})", consumer_group, partition_list, partition_list.len());
    info!("[{}] Will attempt to open receivers for partitions: {:?}", consumer_group, partition_list);
    info!("[{}] Note: 404 errors for non-existent partitions are normal and expected", consumer_group);
    
    for partition_id in partition_list {
        // Double-check we're not exceeding MAX_PARTITION_LIMIT
        if partition_id > MAX_PARTITION_LIMIT {
            eprintln!("[ehub-debug-consumer] [{}] ERROR: Attempted to scan partition {} which exceeds limit of {}! Skipping.", consumer_group, partition_id, MAX_PARTITION_LIMIT);
            error!("[{}] ERROR: Attempted to scan partition {} which exceeds limit of {}! Skipping.", consumer_group, partition_id, MAX_PARTITION_LIMIT);
            continue;
        }
        
        // CRITICAL: Panic if we ever try to open a partition > 7 - this should be impossible
        if partition_id > MAX_PARTITION_LIMIT {
            eprintln!("[ehub-debug-consumer] [{}] FATAL: Attempted to open partition {} which exceeds limit of {}!", consumer_group, partition_id, MAX_PARTITION_LIMIT);
            eprintln!("[ehub-debug-consumer] [{}] FATAL: min_partition={}, max_partition={}, effective_max={}, absolute_max={}", 
                consumer_group, min_partition, max_partition, effective_max, absolute_max);
            panic!("FATAL: Attempted to open partition {} which exceeds hard limit of {}! This is a bug!", partition_id, MAX_PARTITION_LIMIT);
        }
        
        eprintln!("[ehub-debug-consumer] [{}] Attempting partition {} (limit: {})", consumer_group, partition_id, MAX_PARTITION_LIMIT);
        let partition_str = partition_id.to_string();
        
        // Double-check the partition string is valid
        if let Ok(part_num) = partition_str.parse::<u32>() {
            if part_num > MAX_PARTITION_LIMIT {
                eprintln!("[ehub-debug-consumer] [{}] FATAL: Partition string '{}' parses to {} which exceeds limit!", consumer_group, partition_str, MAX_PARTITION_LIMIT);
                panic!("FATAL: Partition string '{}' exceeds hard limit of {}! This is a bug!", partition_str, MAX_PARTITION_LIMIT);
            }
        }
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
                    let error_msg = format!("{:?}", e);
                    let is_auth_error = error_msg.contains("Unauthorized") || 
                                       error_msg.contains("UnauthorizedAccess") ||
                                       error_msg.contains("Listen");
                    
                    if is_auth_error {
                        error!(
                            "[{}] ❌ AUTHORIZATION ERROR for partition {}: Missing 'Listen' permissions",
                            consumer_group_clone, partition_str
                        );
                        error!("   Check your Shared Access Policy or Service Principal permissions in Azure Portal");
                    } else {
                        warn!(
                            "[{}] Failed to create consumer for partition {}: {:?}",
                            consumer_group_clone, partition_str, e
                        );
                    }
                    return;
                }
            };

            // CRITICAL: Check partition before opening receiver
            if let Ok(part_num) = partition_str.parse::<u32>() {
                if part_num > MAX_PARTITION_LIMIT {
                    eprintln!("[ehub-debug-consumer] [{}] FATAL: About to open receiver for partition {} which exceeds limit of {}!", consumer_group_clone, partition_str, MAX_PARTITION_LIMIT);
                    panic!("FATAL: About to open receiver for partition {} which exceeds hard limit of {}! This is a bug!", partition_str, MAX_PARTITION_LIMIT);
                }
            }
            
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
                    // CRITICAL: Double-check after opening
                    if let Ok(part_num) = partition_str.parse::<u32>() {
                        if part_num > MAX_PARTITION_LIMIT {
                            eprintln!("[ehub-debug-consumer] [{}] FATAL: Successfully opened receiver for partition {} which exceeds limit of {}!", consumer_group_clone, partition_str, MAX_PARTITION_LIMIT);
                            panic!("FATAL: Successfully opened receiver for partition {} which exceeds hard limit of {}! This is a bug!", partition_str, MAX_PARTITION_LIMIT);
                        }
                    }
                    
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
                    // Check if this is a 404 (partition doesn't exist)
                    let error_msg = format!("{:?}", e);
                    let is_partition_not_found = error_msg.contains("404") ||
                                                error_msg.contains("could not be found") ||
                                                error_msg.contains("ResourceMgrExceptions");
                    
                    if is_partition_not_found {
                        // Partition doesn't exist - this is NORMAL and EXPECTED
                        // Event Hubs may not have all partitions 0-7 configured
                        info!(
                            "[{}] Partition {} does not exist (this is normal - Event Hub may not have this partition configured)",
                            consumer_group_clone, partition_str
                        );
                    } else {
                        // Some other error - log as warning
                        warn!(
                            "[{}] Could not open receiver for partition {}: {:?}",
                            consumer_group_clone, partition_str, e
                        );
                    }
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
                // Extract meaningful error information
                let error_msg = format!("{:?}", err);
                let is_auth_error = error_msg.contains("Unauthorized") || 
                                   error_msg.contains("UnauthorizedAccess") ||
                                   error_msg.contains("Listen") ||
                                   error_msg.contains("claim");
                
                let is_partition_not_found = error_msg.contains("404") ||
                                            error_msg.contains("could not be found") ||
                                            error_msg.contains("ResourceMgrExceptions");
                
                // Partition not found (404) is expected - just log once and continue
                if is_partition_not_found {
                    // Check if this partition is beyond our expected range
                    if let Ok(part_num) = partition.parse::<u32>() {
                        if part_num > MAX_PARTITION_LIMIT {
                            eprintln!("[ehub-debug-consumer] [{}:{}] ERROR: Got 404 for partition {} which is > {}! This should not happen - check MAX_PARTITION env var!", consumer_group, partition, part_num, MAX_PARTITION_LIMIT);
                            error!("[{}:{}] ERROR: Got 404 for partition {} which exceeds limit of {}! Check MAX_PARTITION configuration.", consumer_group, partition, part_num, MAX_PARTITION_LIMIT);
                            error!("[{}:{}] This means the code tried to access a partition beyond the hard limit - there's a bug!", consumer_group, partition);
                        } else {
                            // Partition is in valid range (0-7) but doesn't exist - this is NORMAL
                            // Event Hubs may not have all partitions configured
                            info!(
                                "[{}:{}] Partition {} does not exist (this is normal - Event Hub may not have this partition configured)",
                                consumer_group, partition, part_num
                            );
                        }
                    } else {
                        // Couldn't parse partition number, but it's a 404 so it's probably fine
                        info!(
                            "[{}:{}] Partition does not exist (this is normal - Event Hub may not have this partition configured)",
                            consumer_group, partition
                        );
                    }
                    // Don't record as error, just return gracefully
                    return Ok(());
                }
                
                if is_auth_error {
                    error!("");
                    error!("╔═══════════════════════════════════════════════════════════════════════════════╗");
                    error!("║ ⚠️  AUTHORIZATION ERROR                                                       ║");
                    error!("╠═══════════════════════════════════════════════════════════════════════════════╣");
                    error!("║ Consumer Group: {:<65} ║", consumer_group);
                    error!("║ Partition:      {:<65} ║", partition);
                    error!("║                                                                               ║");
                    error!("║ The consumer does not have 'Listen' permissions on this Event Hub.           ║");
                    error!("║                                                                               ║");
                    error!("║ SOLUTION:                                                                     ║");
                    error!("║ 1. Go to Azure Portal → Event Hub Namespace → Shared access policies        ║");
                    error!("║ 2. Ensure your policy has 'Listen' permission                                ║");
                    error!("║ 3. If using Service Principal, grant 'Azure Event Hubs Data Receiver' role   ║");
                    error!("║                                                                               ║");
                    if error_msg.contains("TrackingId") {
                        let tracking_id = error_msg
                            .split("TrackingId:")
                            .nth(1)
                            .and_then(|s| s.split(',').next())
                            .unwrap_or("N/A");
                        error!("║ Tracking ID:    {:<65} ║", tracking_id);
                    }
                    error!("╚═══════════════════════════════════════════════════════════════════════════════╝");
                    error!("");
                } else {
                    error!("");
                    error!("╔═══════════════════════════════════════════════════════════════════════════════╗");
                    error!("║ ❌ ERROR RECEIVING EVENT                                                      ║");
                    error!("╠═══════════════════════════════════════════════════════════════════════════════╣");
                    error!("║ Consumer Group: {:<65} ║", consumer_group);
                    error!("║ Partition:      {:<65} ║", partition);
                    error!("║ Timestamp:      {:<65} ║", receive_timestamp.format("%Y-%m-%d %H:%M:%S%.3f UTC"));
                    error!("║                                                                               ║");
                    // Try to extract a readable error message
                    let readable_error = if error_msg.len() > 200 {
                        format!("{}...", &error_msg[..197])
                    } else {
                        error_msg.clone()
                    };
                    error!("║ Error:                                                                        ║");
                    for line in readable_error.lines() {
                        let display_line = if line.len() > 75 {
                            format!("{}...", &line[..72])
                        } else {
                            line.to_string()
                        };
                        error!("║   {:<75} ║", display_line);
                    }
                    error!("╚═══════════════════════════════════════════════════════════════════════════════╝");
                    error!("");
                }
                
                // Update error statistics
                {
                    let mut stats_guard = stats.lock().await;
                    if let Some(stat) = stats_guard.get_mut(&stats_key) {
                        stat.record_error();
                    }
                }
            }
        }
    }

    warn!(
        "[{}:{}] Event stream ended",
        consumer_group, partition
    );

    Ok(())
}
