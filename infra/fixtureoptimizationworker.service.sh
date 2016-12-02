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
ExecStart=cd /kohls/apps/fo/fo_worker/infra && chmod +x worker.sh && ./worker.sh
ExecStop
# ExecReload
# Restart
# RemainAfterExit