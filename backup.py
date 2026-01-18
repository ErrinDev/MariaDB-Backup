import os
import sys
import yaml
import subprocess
import datetime
import argparse
import requests
import time
import glob
import shutil
from pathlib import Path

# ANSI Color Codes
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def load_config():
    if not os.path.exists("config.yml"):
        print(f"{RED}Config file 'config.yml' not found.{RESET}")
        sys.exit(1)
    try:
        with open("config.yml", "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"{RED}Error loading config.yml: {e}{RESET}")
        sys.exit(1)

def send_discord_notification(config, message):
    webhook_url = config.get("discord", {}).get("webhook_url")
    if not webhook_url or webhook_url == "YOUR_DISCORD_WEBHOOK_URL":
        return
    
    try:
        requests.post(webhook_url, json={"content": message})
    except Exception as e:
        print(f"{RED}Error sending discord notification: {e}{RESET}")

def get_retention_policy(config, db_name):
    retention = config.get("retention", {})
    overrides = retention.get("overrides", {})
    if db_name in overrides:
        return overrides[db_name]
    return retention.get("default", {"keep_last": 10, "max_gb": 5.0})

def apply_retention(config, host, db_name):
    storage_path = Path(config.get("storage", {}).get("path", "./backups"))
    db_backup_dir = storage_path / host
    if not db_backup_dir.exists():
        return

    policy = get_retention_policy(config, db_name)
    keep_last = policy.get("keep_last", 10)
    max_bytes = policy.get("max_gb", 5.0) * 1024 * 1024 * 1024

    # Count based retention
    # Use a more specific glob to avoid matching databases that share a prefix
    # Pattern: db_name-DD-MM-YYYY-N.sql.gz
    backups = sorted(
        [f for f in db_backup_dir.glob(f"{db_name}-[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]-*.sql.gz")],
        key=os.path.getmtime,
        reverse=True
    )

    to_delete = backups[keep_last:]
    for f in to_delete:
        f.unlink()
        print(f"{YELLOW}Deleted old backup (count limit): {f}{RESET}")
    
    # Refresh backups list for size-based check
    backups = sorted(
        [f for f in db_backup_dir.glob(f"{db_name}-[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]-*.sql.gz")],
        key=os.path.getmtime,
        reverse=True
    )
    
    # Size based retention
    total_size = sum(f.stat().st_size for f in backups)
    
    # Calculate total size of all backups in this host directory to warn about stale files
    all_files = list(db_backup_dir.glob("*.sql.gz"))
    host_total_size = sum(f.stat().st_size for f in all_files)
    stale_size = host_total_size - total_size
    if stale_size > 10 * 1024 * 1024: # More than 10MB of potentially stale files
        print(f"{YELLOW}Note: {stale_size / (1024*1024):.2f} MB of other backup files found in {db_backup_dir} (not managed by {db_name} policy){RESET}")

    if backups and total_size > max_bytes:
        print(f"{CYAN}Size limit exceeded for {db_name} ({total_size / (1024**3):.2f}GB > {max_bytes / (1024**3):.2f}GB). Pruning...{RESET}")
    while total_size > max_bytes and backups:
        oldest = backups.pop()
        total_size -= oldest.stat().st_size
        oldest.unlink()
        print(f"{YELLOW}Deleted old backup (size limit): {oldest}{RESET}")

def run_backup(config, host, user, password, db_name, port=3306, container=None, timeout=3600):
    storage_path = Path(config.get("storage", {}).get("path", "./backups"))
    db_backup_dir = storage_path / host
    db_backup_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.datetime.now().strftime("%d-%m-%Y")
    
    # Handle multiple backups on same day
    existing_backups = glob.glob(str(db_backup_dir / f"{db_name}-{date_str}-*.sql.gz"))
    n = 1
    if existing_backups:
        # Extract N from filenames and find max
        nums = []
        for b in existing_backups:
            try:
                num_part = Path(b).name.split("-")[-1].split(".")[0]
                nums.append(int(num_part))
            except (ValueError, IndexError):
                continue
        if nums:
            n = max(nums) + 1

    filename = f"{db_name}-{date_str}-{n}.sql.gz"
    filepath = db_backup_dir / filename

    print(f"{CYAN}Backing up {db_name} from {host} to {filepath} (timeout: {timeout}s)...{RESET}")
    
    try:
        env = os.environ.copy()
        env["MYSQL_PWD"] = password
        
        # Using pipe to gzip to save space immediately
        if container:
            # When using docker exec, we pass MYSQL_PWD to the environment inside the container
            # We don't use -it because it's not an interactive shell
            dump_cmd = ["docker", "exec", "-e", f"MYSQL_PWD={password}", container, "mariadb-dump", "-h", host, "-P", str(port), "-u", user, db_name]
        else:
            dump_cmd = ["mariadb-dump", "-h", host, "-P", str(port), "-u", user, db_name]
        
        gzip_cmd = ["gzip"]
        
        with open(filepath, "wb") as f:
            p1 = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env if not container else None)
            p2 = subprocess.Popen(gzip_cmd, stdin=p1.stdout, stdout=f)
            p1.stdout.close()
            
            # Wait for p1 to finish or timeout
            try:
                _, stderr = p1.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                p1.kill()
                p2.kill()
                raise Exception(f"Backup process timed out after {timeout} seconds.")
            
            p2.wait()

        if p1.returncode != 0:
            raise Exception(stderr.decode().strip())
        
        if p2.returncode != 0:
            raise Exception("Compression failed.")

        print(f"{GREEN}Backup completed: {filepath}{RESET}")
        
        success_msg = config.get("discord", {}).get("on_success", "Backup of {database} on {host} completed successfully.")
        send_discord_notification(config, success_msg.format(database=db_name, host=host))
        
        apply_retention(config, host, db_name)

    except Exception as e:
        error_str = str(e)
        if "[Errno 2] No such file or directory: 'mariadb-dump'" in error_str:
            error_str = "mariadb-dump not found. Please install mariadb-client or configure to use docker exec."
        print(f"{RED}Backup failed: {error_str}{RESET}")
        failure_msg = config.get("discord", {}).get("on_failure", "Backup of {database} on {host} failed: {error}")
        send_discord_notification(config, failure_msg.format(database=db_name, host=host, error=error_str))
        if filepath.exists():
            filepath.unlink()

def list_backups(config):
    storage_path = Path(config.get("storage", {}).get("path", "./backups"))
    if not storage_path.exists():
        print(f"{YELLOW}No backups found.{RESET}")
        return

    print(f"{CYAN}{'Host':<30} {'Backup Name':<50} {'Size':<10} {'Date':<20}{RESET}")
    print("-" * 110)
    
    for host_dir in storage_path.iterdir():
        if host_dir.is_dir():
            for backup_file in sorted(host_dir.glob("*.sql.gz")):
                size_mb = backup_file.stat().st_size / (1024 * 1024)
                mtime = datetime.datetime.fromtimestamp(backup_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                # Format for restore command: host/backup_name (without extension)
                print(f"{host_dir.name:<30} {backup_file.name:<50} {size_mb:>8.2f} MB {mtime:<20}")

def restore_backup(config, backup_ref):
    # backup_ref format: db_server_one_FQDN/database-DD-MM-YYYY-N
    try:
        host, backup_name = backup_ref.split("/")
        if not backup_name.endswith(".sql.gz"):
             backup_file_path = Path(config.get("storage", {}).get("path", "./backups")) / host / f"{backup_name}.sql.gz"
        else:
             backup_file_path = Path(config.get("storage", {}).get("path", "./backups")) / host / backup_name
    except ValueError:
        print(f"{RED}Invalid backup reference format. Use host/backup_name{RESET}")
        return

    if not backup_file_path.exists():
        print(f"{RED}Backup file not found: {backup_file_path}{RESET}")
        return

    db_name = backup_name.split("-")[0]
    
    # Find server config for this host
    server_cfg = None
    for s in config.get("servers", []):
        if s["host"] == host:
            server_cfg = s
            break
    
    if not server_cfg:
        print(f"{RED}No server configuration found for host {host}{RESET}")
        return

    print(f"{CYAN}Restoring {db_name} on {host} from {backup_file_path}...{RESET}")
    
    try:
        env = os.environ.copy()
        env["MYSQL_PWD"] = server_cfg['password']
        
        # zcat backup.sql.gz | mariadb -h host -P port -u user db_name
        zcat_cmd = ["zcat", str(backup_file_path)]
        
        if server_cfg.get("container"):
             # We use -i for piping stdin, but NOT -t
             mysql_cmd = ["docker", "exec", "-i", "-e", f"MYSQL_PWD={server_cfg['password']}", server_cfg["container"], "mariadb", "-u", server_cfg["user"], db_name]
        else:
             mysql_cmd = ["mariadb", "-h", host, "-P", str(server_cfg.get("port", 3306)), "-u", server_cfg["user"], db_name]
        
        p1 = subprocess.Popen(zcat_cmd, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(mysql_cmd, stdin=p1.stdout, stderr=subprocess.PIPE, env=env if not server_cfg.get("container") else None)
        p1.stdout.close()
        _, stderr = p2.communicate()

        if p2.returncode != 0:
            raise Exception(stderr.decode().strip())

        print(f"{GREEN}Restore completed successfully.{RESET}")
    except Exception as e:
        print(f"{RED}Restore failed: {e}{RESET}")

def run_all_now(config):
    for server in config.get("servers", []):
        for db_entry in server.get("databases", []):
            db_name = db_entry
            db_timeout = server.get("timeout", 3600)
            
            if isinstance(db_entry, dict):
                db_name = db_entry.get("name")
                db_timeout = db_entry.get("timeout", db_timeout)

            run_backup(
                config, 
                server["host"], 
                server["user"], 
                server["password"], 
                db_name, 
                port=server.get("port", 3306),
                container=server.get("container"),
                timeout=db_timeout
            )

def main():
    parser = argparse.ArgumentParser(description="MariaDB Backup System")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list", help="List all available backups")
    
    restore_parser = subparsers.add_parser("restore", help="Restore a backup")
    restore_parser.add_argument("backup_ref", help="Backup reference in host/filename format")

    subparsers.add_parser("now", help="Run all backups immediately")
    
    # For simplicity, if no command, we could run the scheduler. 
    # But usually a CLI tool shouldn't block. 
    # I'll add a 'daemon' command.
    subparsers.add_parser("daemon", help="Run the backup scheduler")

    args = parser.parse_args()
    config = load_config()

    if args.command == "list":
        list_backups(config)
    elif args.command == "restore":
        restore_backup(config, args.backup_ref)
    elif args.command == "now":
        run_all_now(config)
    elif args.command == "daemon":
        print(f"{GREEN}Starting backup daemon...{RESET}")
        # Simple scheduling loop
        last_run = {} # (host, db) -> last_run_time
        
        while True:
            now = datetime.datetime.now()
            for server in config.get("servers", []):
                host = server["host"]
                for db_entry in server.get("databases", []):
                    db_name = db_entry
                    db_timeout = server.get("timeout", 3600)
                    
                    if isinstance(db_entry, dict):
                        db_name = db_entry.get("name")
                        db_timeout = db_entry.get("timeout", db_timeout)

                    should_run = False
                    key = (host, db_name)
                    
                    if "schedule" in server:
                        # Daily at set time HH:MM
                        try:
                            sched_time = datetime.datetime.strptime(server["schedule"], "%H:%M").time()
                            today_run_time = datetime.datetime.combine(now.date(), sched_time)
                            
                            if now >= today_run_time:
                                if key not in last_run or last_run[key] < today_run_time:
                                    should_run = True
                        except ValueError:
                            print(f"{RED}Invalid schedule format for {host}/{db_name}: {server['schedule']}{RESET}")
                    
                    elif "interval_hours" in server:
                        interval = datetime.timedelta(hours=server["interval_hours"])
                        if key not in last_run or (now - last_run[key]) >= interval:
                            should_run = True
                    
                    if should_run:
                        run_backup(
                            config, 
                            host, 
                            server["user"], 
                            server["password"], 
                            db_name, 
                            port=server.get("port", 3306),
                            container=server.get("container"),
                            timeout=db_timeout
                        )
                        last_run[key] = now
            
            time.sleep(60)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
