# Running on Windows

## Prerequisites

1. **Install Rust**: Download and install from https://www.rust-lang.org/tools/install
   - Or use: `winget install Rustlang.Rustup` (Windows 11/10 with winget)

2. **Install Git** (if not already installed): https://git-scm.com/download/win

## Quick Start

### Option 1: Using PowerShell (Recommended)

1. **Open PowerShell** (as Administrator if needed)

2. **Clone or navigate to the project**:
   ```powershell
   cd C:\path\to\ehub_debug_consumer
   ```

3. **Set environment variables** (PowerShell syntax):
   ```powershell
   $env:EVENTHUBS_HOST = "your-namespace.servicebus.windows.net"
   $env:EVENTHUB_NAME = "your-eventhub-name"
   $env:CONSUMER_GROUPS = "Default"
   $env:MIN_PARTITION = "0"
   $env:MAX_PARTITION = "7"
   
   # Authentication - Choose ONE method:
   
   # Method 1: Connection String (Easier)
   $env:EVENTHUB_CONNECTION_STRING = "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy-name;SharedAccessKey=key"
   
   # Method 2: Service Principal
   $env:AZURE_CLIENT_ID = "your-client-id"
   $env:AZURE_CLIENT_SECRET = "your-client-secret"
   $env:AZURE_TENANT_ID = "your-tenant-id"
   
   # Optional: Set log level
   $env:RUST_LOG = "info,azure_messaging_eventhubs=warn,azure_core=warn"
   ```

4. **Build and run**:
   ```powershell
   cargo run --release
   ```

### Option 2: Using Command Prompt (CMD)

1. **Open Command Prompt**

2. **Navigate to project**:
   ```cmd
   cd C:\path\to\ehub_debug_consumer
   ```

3. **Set environment variables** (CMD syntax):
   ```cmd
   set EVENTHUBS_HOST=your-namespace.servicebus.windows.net
   set EVENTHUB_NAME=your-eventhub-name
   set CONSUMER_GROUPS=Default
   set MIN_PARTITION=0
   set MAX_PARTITION=7
   
   REM Authentication - Choose ONE:
   
   REM Method 1: Connection String
   set EVENTHUB_CONNECTION_STRING=Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy-name;SharedAccessKey=key
   
   REM Method 2: Service Principal
   set AZURE_CLIENT_ID=your-client-id
   set AZURE_CLIENT_SECRET=your-client-secret
   set AZURE_TENANT_ID=your-tenant-id
   
   REM Optional: Log level
   set RUST_LOG=info,azure_messaging_eventhubs=warn,azure_core=warn
   ```

4. **Build and run**:
   ```cmd
   cargo run --release
   ```

### Option 3: Using a .bat Script

Create a file `run.bat`:

```batch
@echo off
set EVENTHUBS_HOST=your-namespace.servicebus.windows.net
set EVENTHUB_NAME=your-eventhub-name
set CONSUMER_GROUPS=Default
set MIN_PARTITION=0
set MAX_PARTITION=7

REM Choose authentication method:
set EVENTHUB_CONNECTION_STRING=Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy-name;SharedAccessKey=key

REM Or use Service Principal:
REM set AZURE_CLIENT_ID=your-client-id
REM set AZURE_CLIENT_SECRET=your-client-secret
REM set AZURE_TENANT_ID=your-tenant-id

set RUST_LOG=info,azure_messaging_eventhubs=warn,azure_core=warn

cargo run --release
```

Then just run:
```cmd
run.bat
```

## Building Release Binary

To build a release binary that you can run directly:

```powershell
cargo build --release
```

The binary will be at: `target\release\ehub_debug_consumer.exe`

You can then run it directly:
```powershell
.\target\release\ehub_debug_consumer.exe
```

## Troubleshooting

### "cargo: command not found"
- Make sure Rust is installed and in your PATH
- Restart your terminal after installing Rust
- Verify with: `cargo --version`

### "Permission denied" or authentication errors
- Check your connection string or Service Principal credentials
- Ensure the Shared Access Policy has "Listen" permission
- For Service Principal, ensure it has "Azure Event Hubs Data Owner" role

### "Connection refused" or network errors
- Check your firewall settings
- Verify `EVENTHUBS_HOST` is correct (should end with `.servicebus.windows.net`)
- Ensure your network allows outbound connections to Azure Service Bus

### Environment variables not working
- In PowerShell, use `$env:VARIABLE_NAME = "value"`
- In CMD, use `set VARIABLE_NAME=value`
- Variables set in one terminal session don't persist to other sessions
- To make them permanent, set them in System Environment Variables (Windows Settings)

## Example Full Command (PowerShell)

```powershell
$env:EVENTHUBS_HOST = ""
$env:EVENTHUB_NAME = ""
$env:CONSUMER_GROUPS = ""
$env:MIN_PARTITION = "0"
$env:MAX_PARTITION = "7"
$env:AZURE_CLIENT_ID = ""
$env:AZURE_CLIENT_SECRET = ""
$env:AZURE_TENANT_ID = ""
$env:RUST_LOG = "info,azure_messaging_eventhubs=warn,azure_core=warn"
cargo run --release
```

## Stopping the Application

Press `Ctrl+C` in the terminal to stop the application gracefully.

