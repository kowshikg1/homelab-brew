import pytest

# Requires a real HDD mountpoint and a configured rclone remote.
# Verify manually:
#   python src/scripts/services/log_backup.py --target hdd
#   python src/scripts/services/log_backup.py --target cloud


pytestmark = pytest.mark.skip(reason='requires live HDD and rclone environment')
