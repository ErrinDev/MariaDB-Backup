import unittest
import os
import shutil
import yaml
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path so we can import backup.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backup import get_retention_policy, apply_retention, load_config

# ANSI Color Codes
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

class TestBackupLogic(unittest.TestCase):
    def setUp(self):
        print(f"\n{CYAN}Setting up test: {self._testMethodName}{RESET}")
        self.test_dir = Path("./test_backups")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = Path("test_config.yml")
        self.config = {
            "storage": {"path": str(self.test_dir)},
            "retention": {
                "default": {"keep_last": 2, "max_gb": 0.000001}, # small size for testing
                "overrides": {
                    "important_db": {"keep_last": 5, "max_gb": 1.0}
                }
            }
        }
        with open(self.config_file, "w") as f:
            yaml.dump(self.config, f)

    def tearDown(self):
        print(f"{CYAN}Tearing down test: {self._testMethodName}{RESET}")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        if self.config_file.exists():
            self.config_file.unlink()

    def test_get_retention_policy(self):
        print(f"{YELLOW}Action: Checking retention policy retrieval...{RESET}")
        policy = get_retention_policy(self.config, "website_db")
        self.assertEqual(policy["keep_last"], 2)
        print(f"{GREEN}Result: Default policy correctly retrieved.{RESET}")

        policy = get_retention_policy(self.config, "important_db")
        self.assertEqual(policy["keep_last"], 5)
        print(f"{GREEN}Result: Override policy correctly retrieved.{RESET}")

    def test_apply_retention_count(self):
        print(f"{YELLOW}Action: Testing retention by count...{RESET}")
        host = "test_host"
        db = "test_db"
        db_dir = self.test_dir / host
        db_dir.mkdir(parents=True)

        # Create 5 dummy backup files with proper naming for the new glob pattern
        # Pattern: db_name-DD-MM-YYYY-N.sql.gz
        for i in range(5):
            f = db_dir / f"{db}-18-01-2026-{i}.sql.gz"
            f.write_text("dummy content")
            # Ensure different mtimes
            os.utime(f, (time.time() + i, time.time() + i))
        
        print(f"Created 5 dummy backups in {db_dir}")
        
        # Policy is keep_last: 2
        apply_retention(self.config, host, db)
        
        remaining = list(db_dir.glob(f"{db}-*.sql.gz"))
        print(f"Backups remaining after retention: {len(remaining)}")
        self.assertEqual(len(remaining), 2)
        print(f"{GREEN}Result: Correctly kept only the last 2 backups.{RESET}")

    @patch('requests.post')
    def test_discord_notification(self, mock_post):
        from backup import send_discord_notification
        print(f"{YELLOW}Action: Testing Discord notification...{RESET}")
        config = {"discord": {"webhook_url": "http://fake-webhook"}}
        send_discord_notification(config, "Test Message")
        mock_post.assert_called_once()
        print(f"{GREEN}Result: Discord notification call verified.{RESET}")

if __name__ == "__main__":
    unittest.main(verbosity=2)
