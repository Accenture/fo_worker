# Source global definitions
if [ -f /etc/bashrc ]; then
       . /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

PATH=/var/lib/anaconda3/bin:$PATH:$HOME/.local/bin:$HOME/bin
export PATH

# FO ENV VARS
export RMQ_HOST="nl001527.tst.kohls.com"
export RMQ_PORT="5672"
export MONGO_HOST="nzf1x0p@nl001535.tst.kohls.com"
export MONGO_PORT="27020"
export MONGO_NAME="app"
#export MONGO_USERNAME=""
#export MONGO_PASSWORD=""
export FO_WORKER_NUM_PROCESSES="3"

export FO_NOTIFICATIONS_HOST="nl001527.tst.kohls.com"
export FO_NOTIFICATIONS_PORT="8000"

export FO_API_HOST="nl001527.tst.kohls.com"
export FO_API_PORT="5000"

# User specific aliases and functions
