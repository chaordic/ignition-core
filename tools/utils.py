#!/usr/bin/env python
import logging
import boto.ec2
import sys
import subprocess
import select
import time

logging.basicConfig(level=logging.INFO)

def get_active_instances(conn):
    active = [instance for res in conn.get_all_instances()
              for instance in res.instances
              if instance.state in set(['pending', 'running',
                                        'stopping', 'stopped'])]
    return active

def parse_nodes(active_instances, cluster_name):
    master_nodes = []
    slave_nodes = []
    for instance in active_instances:
        group_names = [g.name for g in instance.groups]
        # This can handle both spark-ec2 and flintrock clusters
        if (cluster_name + "-master") in group_names or (("flintrock-" + cluster_name) in group_names and instance.tags.get('flintrock-role') == 'master'):
            master_nodes.append(instance)
        elif (cluster_name + "-slaves") in group_names or (("flintrock-" + cluster_name) in group_names and instance.tags.get('flintrock-role') in ('slave', None)):
            slave_nodes.append(instance)
    return (master_nodes, slave_nodes)

def get_masters(cluster_name, region):
    conn = boto.ec2.connect_to_region(region)

    active = get_active_instances(conn)
    master_nodes, slave_nodes = parse_nodes(active, cluster_name)
    return master_nodes

def get_active_nodes(cluster_name, region):
    conn = boto.ec2.connect_to_region(region)
    active = get_active_instances(conn)
    return parse_nodes(active, cluster_name)


def tag_instances(cluster_name, tags, region):
    conn = boto.ec2.connect_to_region(region)

    active = get_active_instances(conn)
    logging.info('%d active instances', len(active))

    master_nodes, slave_nodes = parse_nodes(active, cluster_name)
    logging.info('%d master, %d slave', len(master_nodes), len(slave_nodes))

    if master_nodes:
        conn.create_tags([i.id for i in master_nodes],
                         {'spark_node_type': 'master'})
    if slave_nodes:
        conn.create_tags([i.id for i in slave_nodes],
                         {'spark_node_type': 'slave'})

    if slave_nodes or master_nodes:
        ids = [i.id for l in (master_nodes, slave_nodes) for i in l]
        conn.create_tags(ids, tags)

    logging.info("Tagged nodes.")

class ProcessTimeoutException(Exception): pass

def read_from_to(_from, to):
    data = read_non_blocking(_from)
    read_data = False
    while data is not None:
        read_data = True
        to.write(data)
        data = read_non_blocking(_from)
    to.flush()
    return read_data

def read_non_blocking(f):
    result = []
    while select.select([f], [], [], 0)[0]:
        c = f.read(1)
        if c:
            result.append(c)
        else:
            break
    return ''.join(result) if result else None

def check_call_with_timeout(args, stdin=None, stdout=None,
                            stderr=None, shell=False,
                            timeout_total_minutes=0,
                            timeout_inactivity_minutes=0):
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    begin_time_total = time.time()
    begin_time_inactivity = time.time()
    p = subprocess.Popen(args,
                         stdin=stdin,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=shell,
                         universal_newlines=False)
    while True:
        if read_from_to(p.stdout, stdout):
            begin_time_inactivity = time.time()
        if read_from_to(p.stderr, stderr):
            begin_time_inactivity = time.time()
        if p.poll() is not None:
            break
        terminate_by_total_timeout = timeout_total_minutes > 0 and time.time() - begin_time_total > (timeout_total_minutes * 60)
        terminate_by_inactivity_timeout = timeout_inactivity_minutes > 0 and time.time() - begin_time_inactivity > (timeout_inactivity_minutes * 60)
        if terminate_by_inactivity_timeout or terminate_by_total_timeout:
            p.terminate()
            for i in range(100):
                if p.poll is not None:
                    break
                time.sleep(0.1)
            p.kill()
            message = 'Terminated by inactivity' if terminate_by_inactivity_timeout else 'Terminated by total timeout'
            raise ProcessTimeoutException(message)
        time.sleep(0.5)
    read_from_to(p.stdout, stdout)
    read_from_to(p.stderr, stderr)
    if p.returncode != 0:
        stdall = 'STDOUT:\n{}\nSTDERR:\n{}'.format(stdout, stderr)
        raise subprocess.CalledProcessError(p.returncode, args, output=stdall)
    return p.returncode
