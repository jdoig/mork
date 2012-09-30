import work
import zmq
import sys
import json
from random import randint
from kazoo.protocol.states import EventType
from exceptions import SystemExit, KeyboardInterrupt
from kazoo.exceptions import NoNodeError, NotEmptyError
from zk_client import ZkClient

zk_address = "127.0.0.1:2181" if len(sys.argv) < 3 else sys.argv[2]
zk = ZkClient(zk_address, sys.argv[1])
context = zmq.Context()
work_receiver = context.socket(zmq.PULL)
name = ""

def select_master():
    """ select a master at random from the masters that are registers in zk """
    masters = zk.get_masters()
    idx = randint(0,(len(masters)-1))
    return masters[idx]

def get_data(master_name):
    """ get the masters json meta data and return as a dictionary """
    data, _ =  zk.get_master(master_name)
    return json.loads(data)

def re_enlist(zk_event):
    """ callback to re-enlist worker if his master is deleted  """
    type, _, _ = zk_event
    if type == EventType.DELETED:
        master = enlist()
        data = json.loads(zk.get(name)[0]) # get old data
        data['master'] = master # replace master with new one
        zk.set(name,json.dumps(data)) # set data

def enlist():
    """
    enlist with a master by pulling the master list from zk,
    selecting a random one and setting up a 0MQ pull channel.
    watch the master for deletion and re-enlist with another if he disappears.
    """
    # 1: select master
    master_name = select_master()
    master_data = get_data(master_name)
    print master_name

    # 2: connect to master via 0MQ (zmq.PULL)
    work_receiver.connect("tcp://" + master_data['listening_on'])

    return master_name

def execute(job, name):
    """ mark job as in progress, processes and then delete the job """
    # 1: create 'in-progress' node under job
    try:
        zk.mark_job(job,name) # mark job as in progress
        work.Do(query) # perform the Do action in work.py
        zk.delete_job(job) # remove job when completed

    except (NoNodeError, NotEmptyError):
        print "dang! someone beat me to it"

if __name__ == "__main__":
    master = enlist()
    name = zk.create_worker(master, re_enlist)
    short_name = name.split('/')[-1]
    try:
        while True:
            query = work_receiver.recv()
            execute(query, short_name)

    except (KeyboardInterrupt, SystemExit):
        print "exiting " + name

    finally:
        zk.stop()
        work_receiver.close()
        context.term()
        sys.exit()
