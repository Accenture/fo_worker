for pid in `ps -ef | grep src/worker.py | grep -v grep | awk '{print \$2}'`
do
    echo $pid
    kill -9 $pid
done;
