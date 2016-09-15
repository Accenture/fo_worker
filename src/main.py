#!/usr/bin/env python
# -*- coding: utf-8 -*-

from doer import main as start_worker
from multiprocessing import Process
from os import environ as env
import logging

#
# python src/main.py --log-level info --num-processes 3
#

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOG_FILE = None
LOGGER = logging.getLogger(__name__)


def main(num_processes):
    """
    Use a list of Processes since daemonic processes can't spwan children
    :param num_processes:
    :return:
    """
    pool = []
    try:
        for _ in range(num_processes):
            p = Process(target=start_worker)
            pool.append(p)
            p.start()
    except KeyboardInterrupt:
        for p in pool:
            p.terminate()
            p.join()

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num-processes",
                        type=int,
                        help="Number of processes (consumers) to allot to this worker instance",
                        default=None)
    parser.add_argument("-l", "--log-level",
                        type=str,
                        default='info',
                        help="log level in [`critical` | `error` | `warn` | `info` | `debug`]")

    args = parser.parse_args()

    if args.log_level is not None:
        levels = dict(CRITICAL=50,
                      ERROR=40,
                      WARN=30,
                      INFO=20,
                      DEBUG=10)
        args.log_level = levels.get(args.log_level.upper())
    else:
        level = args.log_level.INFO

    logging.basicConfig(level=args.log_level,
                        format=LOG_FORMAT)

    if not args.num_processes:
        args.num_processes = env.get('FO_WORKER_NUM_PROCESSES', 3)

    main(args.num_processes)
