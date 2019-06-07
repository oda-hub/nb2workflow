from __future__ import print_function

import os
import json
import glob
import time
import logging
import inspect
import requests
import base64
import hashlib
import datetime
import tempfile
import nbformat


logger=logging.getLogger('nb2workflow.health')

def current_health():
    issues=[]
    status = {}

    statvfs = os.statvfs(".")
    status['fs_space'] = dict(
        size_mb = statvfs.f_frsize * statvfs.f_blocks / 1024 / 1024,
        avail_mb = statvfs.f_frsize * statvfs.f_bavail / 1024 / 1024,
    )

    if status['fs_space']['avail_mb'] < 300:
        issues.append("not enough free space: %.5lg Mb left"%status['fs_space']['avail_mb'])


    import psutil

    processes = []
    status['n_open_files'] = 0
    status['n_processes'] = 0
    status['n_threads'] = 0

    for proc in psutil.process_iter():
        try:
            processes.append(dict(
                    n_open_files = len(proc.open_files()),
                ))

            status['n_open_files'] += len(proc.open_files())
            status['n_processes'] += 1
            status['n_threads'] += proc.num_threads()
        except Exception as e:
            pass

    status['cpu_times'] = dict(psutil.cpu_times_percent()._asdict())

    status['loadavg'] = psutil.getloadavg()

    if max(status['loadavg']) > 10:
        issues.append("high load avg: %s"%repr(status['loadavg']))


    status['disk_usage'] = dict([ (k+"_mb", v/1024/1024) if k!="percent" else (k,v) for k,v in dict(psutil.disk_usage(".")._asdict()).items()])

    #status['processes'] = processes

    return status, issues

