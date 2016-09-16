#  This is the systemd init for fixture optimization worker

[Unit]
Description=Fixture Optimization Worker
Documentation=https://github.kohls.com/EIM/fo_worker
# After
# Requires
# Wants
# Conflicts

[Service]
Type=simple
ExecStart=python /kohls/apps/fo/fo_worker/src/main.py --log-level info
ExecStop
# ExecReload
# Restart
# RemainAfterExit
