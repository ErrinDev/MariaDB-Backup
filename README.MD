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
  - host: "127.0.0.1"
    container: "db_server_one" # Optional: uses docker exec if specified
    user: "backup_user"
    password: "backup_password"
    databases:
      - "website_db"
    schedule: "02:00" # Daily at 02:00
  - host: "127.0.0.1"
    container: "db_server_two"
    user: "backup_user"
    password: "backup_password"
    databases:
      - "app_db"
    interval_hours: 6 # Every 6 hours
```

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

## Testing

Run the unit tests using the provided script:
```bash
./run_tests.sh
```

## License

MIT
