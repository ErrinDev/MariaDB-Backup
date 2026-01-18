# MariaDB Backup System

A Python-based system for automated MariaDB backups with retention management, Discord notifications, and Docker support.

## Features

- **Automated Backups**: Schedule backups at specific times or intervals.
- **Retention Policy**: Automatically manage disk space by keeping a set number of backups or limiting total backup size (per database).
- **Discord Integration**: Receive real-time notifications for successful backups or failures.
- **Docker Support**: Can perform backups by executing `mariadb-dump` inside Docker containers.
- **Compression**: Backups are compressed using `gzip` to save space.
- **Restore Capability**: Easily restore databases from compressed backups via CLI.
- **Colored Output**: Readable terminal output for monitoring.

## Prerequisites

- Python 3.x
- `pyyaml` and `requests` libraries
- MariaDB client tools (`mariadb-dump`, `mariadb`) OR Docker installed (if using the container option).

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd "Database Backup"
   ```

2. Install dependencies:
   ```bash
   pip install pyyaml requests
   ```

## Configuration

Configure the system using `config.yml`. A sample structure is provided below:

```yaml
storage:
  path: "./backups"

discord:
  webhook_url: "YOUR_DISCORD_WEBHOOK_URL"
  on_success: "Backup of {database} on {host} completed successfully."
  on_failure: "Backup of {database} on {host} failed: {error}"

retention:
  default:
    keep_last: 10
    max_gb: 5.0
  overrides:
    important_db:
      keep_last: 30
      max_gb: 20.0

servers:
  - host: "test.errin.dev"
    container: "db_server_one" # Optional: uses docker exec if specified
    user: "backup_user"
    password: "backup_password"
    databases:
      - name: "website_db"
        timeout: 30  # Custom 30-second timeout for this specific DB
    schedule: "02:00" # Daily at 02:00
  - host: "127.0.0.1"
    container: "db_server_two"
    user: "backup_user"
    password: "backup_password"
    databases:
      - "app_db"
    interval_hours: 6 # Every 6 hours
    timeout: 600 # Default 10-minute timeout for all DBs on this server
```

### Configuration Options

- **`storage`**:
  - `path`: Directory where backups will be stored.
- **`retention`**:
  - `default`: Default policy for all databases.
    - `keep_last`: Number of most recent backups to keep.
    - `max_gb`: Maximum total size of backups for a single database.
  - `overrides`: Specific policies for individual databases.
- **`servers`**:
  - `host`: Database host address.
  - `container`: (Optional) Docker container name if running MariaDB in Docker.
  - `user`: Database user.
  - `password`: Database password.
  - `databases`: List of databases to backup. Can be a string or an object with `name` and `timeout`.
  - `timeout`: (Optional) Default timeout in seconds for all databases on this server.
  - `schedule`: (Optional) Daily backup time in `HH:MM` format.
  - `interval_hours`: (Optional) Backup interval in hours.

## Usage

The main script is `backup.py`.

### List available backups
```bash
python3 backup.py list
```

### Run all backups immediately
```bash
python3 backup.py now
```

### Restore a backup
```bash
python3 backup.py restore [host]/[backup_filename]
# Example: python3 backup.py restore 127.0.0.1/website_db-18-01-2026-1.sql.gz
```

### Run the backup daemon
This will keep the script running and execute backups according to the schedules/intervals in `config.yml`.
```bash
python3 backup.py daemon
```

## Running 24/7 on Ubuntu (Systemd)

To ensure the backup daemon runs 24/7 and starts automatically after a reboot, it is recommended to use `systemd`.

1. **Create a service file**: Create a file named `/etc/systemd/system/mariadb-backup.service` (you may need `sudo`):
   ```ini
   [Unit]
   Description=MariaDB Backup Daemon
   After=network.target

   [Service]
   Type=simple
   User=your_username
   WorkingDirectory=/path/to/Database Backup
   ExecStart=/usr/bin/python3 backup.py daemon
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```
   *Note: Replace `your_username` and `/path/to/Database Backup` with your actual username and the absolute path to the project.*

2. **Reload systemd**:
   ```bash
   sudo systemctl daemon-reload
   ```

3. **Enable and start the service**:
   ```bash
   sudo systemctl enable mariadb-backup.service
   sudo systemctl start mariadb-backup.service
   ```

4. **Check status**:
   ```bash
   sudo systemctl status mariadb-backup.service
   ```

## Testing

Run the unit tests using the provided script:
```bash
./run_tests.sh
```

## License

MIT
