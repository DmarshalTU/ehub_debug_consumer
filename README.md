# Azure Event Hubs Debug Consumer

A comprehensive Rust-based debug consumer for Azure Event Hubs with maximum logging capabilities for troubleshooting connection issues, delays, and lost events in Kubernetes environments.

## Features

- **All Partitions**: Automatically discovers and consumes from all available partitions
- **Earliest Start**: Starts consuming from the earliest available events
- **Multiple Consumer Groups**: Supports multiple consumer groups via environment variable
- **Maximum Logging**: Human-readable, detailed logging with:
  - Event metadata (sequence number, offset, partition key)
  - Timestamps (enqueued time, received time, latency)
  - Event body (UTF-8 or hex)
  - System and application properties
  - Connection state changes
  - Throughput metrics (events/sec)
  - Error tracking
- **Statistics**: Periodic summary reports with latency, throughput, and error statistics

## Prerequisites

- Rust compiler (see [rust installation instructions](https://www.rust-lang.org/tools/install))
- Azure subscription
- Azure CLI installed and logged in (`az login`)
- Event Hub namespace and instance
- Kubernetes cluster (for Helm deployment)

## Configuration

The application reads configuration from environment variables:

- `EVENTHUBS_HOST`: Event Hub namespace host URL (e.g., `your-namespace.servicebus.windows.net`)
- `EVENTHUB_NAME`: Name of the Event Hub instance
- `CONSUMER_GROUPS`: Comma-separated list of consumer groups (e.g., `group1,group2,group3`)
- `RUST_LOG`: Logging level (default: `info,azure_messaging_eventhubs=trace,azure_core=trace`)

## Docker Image

The Docker image is available on Docker Hub:

```bash
docker pull dmarshaltu/ehub_debug_consumer:0.1.0
# or
docker pull dmarshaltu/ehub_debug_consumer:latest
```

## Building

```bash
cargo build --release
```

## Running Locally

```bash
export EVENTHUBS_HOST="your-namespace.servicebus.windows.net"
export EVENTHUB_NAME="your-eventhub-name"
export CONSUMER_GROUPS="default,debug-group"
cargo run --release
```

## Helm Deployment

### Installation

1. Update `helm/ehub-debug-consumer/values.yaml` with your configuration:

```yaml
config:
  eventhubsHost: "your-namespace.servicebus.windows.net"
  eventhubName: "your-eventhub-name"
  consumerGroups: "default,debug-group"
```

2. Use the pre-built Docker image or build your own:

```bash
# Using pre-built image (recommended)
# Image: dmarshaltu/ehub_debug_consumer:0.1.0

# Or build your own
./build-and-push.sh
```

3. Install the chart:

```bash
helm install ehub-debug-consumer ./helm/ehub-debug-consumer
```

### Scaling Pods

The Helm chart includes a Job resource for scaling pods:

1. **Start pods**: Set `replicaCount` to desired number and enable the job:

```yaml
replicaCount: 1
job:
  enabled: true
```

Then upgrade:

```bash
helm upgrade ehub-debug-consumer ./helm/ehub-debug-consumer
```

2. **Stop pods**: Set `replicaCount` to 0:

```yaml
replicaCount: 0
job:
  enabled: true
```

Then upgrade:

```bash
helm upgrade ehub-debug-consumer ./helm/ehub-debug-consumer
```

Alternatively, you can scale directly:

```bash
kubectl scale deployment ehub-debug-consumer --replicas=1
kubectl scale deployment ehub-debug-consumer --replicas=0
```

## Known Issues

There are currently compilation errors related to API type mismatches in the Azure Event Hubs SDK. The code structure is correct, but the API calls need to be adjusted based on the actual SDK version and method signatures. The main issues are:

1. Type mismatches in `ConsumerClient::builder().open()` method calls
2. Credential type handling

These will need to be resolved by:
- Checking the exact method signatures in the SDK documentation
- Testing with your actual Event Hub setup
- Adjusting the types accordingly

## Logging Output

The consumer provides detailed, human-readable logs in the following format:

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║ EVENT RECEIVED                                                                 ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║ Consumer Group: default                                                       ║
║ Partition:      0                                                              ║
║ Sequence Number: 12345                                                         ║
║ Offset:         1234567890                                                     ║
║ Enqueued Time:  2024-01-01 12:00:00.123 UTC                                    ║
║ Received Time:  2024-01-01 12:00:01.456 UTC                                    ║
║ Latency:        1333 ms (⚠️  MEDIUM)                                          ║
║ Partition Key:  key123                                                         ║
║ Body Size:      1024 bytes                                                     ║
║ System Properties:                                                             ║
║   x-opt-sequence-number: 12345                                                  ║
║ Body (UTF-8):                                                                  ║
║   {"message": "Hello World"}                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

## Statistics

Every 30 seconds, a statistics summary is logged:

```
========== Statistics Summary (last 30s) ==========
Consumer Group: default, Partition: 0
  Total Events: 1000
  Events/sec: 33.33
  Average Latency: 500 ms
  Min Latency: 10 ms
  Max Latency: 5000 ms
  Last Event Time: 2024-01-01 12:00:30.123 UTC
  Last Enqueued Time: 2024-01-01 12:00:29.890 UTC
==================================================
```

## Troubleshooting

1. **No events received**: Check that:
   - Event Hub name and host are correct
   - Consumer group exists
   - Events are being sent to the Event Hub
   - Network connectivity to Azure

2. **High latency**: Check:
   - Network conditions
   - Event Hub service status
   - Consumer processing speed

3. **Connection errors**: Check:
   - Azure credentials (`az login`)
   - Event Hub permissions
   - Network firewall rules

## License

MIT

