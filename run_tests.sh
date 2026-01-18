#!/bin/bash

# ANSI Color Codes
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}Starting MariaDB Backup System Unit Tests...${NC}"
echo "------------------------------------------"

# Run the python unit test script
python3 tests/test_backup_logic.py

EXIT_CODE=$?

echo "------------------------------------------"
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}SUCCESS: All tests passed!${NC}"
else
    echo -e "${RED}FAILURE: Some tests failed.${NC}"
fi

exit $EXIT_CODE
