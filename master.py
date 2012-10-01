import sys
from zk_client import ZkClient
import zmq
from time import sleep
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import EventType
from exceptions import KeyboardInterrupt, SystemExit

zk_address = "127.0.0.1:2181" if len(sys.argv) < 4 else sys.argv[3]
zk = ZkClient(zk_address, sys.argv[1])

context = zmq.Context()
message_client = context.socket(zmq.PUSH)
name = ""


def register(address):
    """create an ephemeral node in ZooKeeper"""

    #set up 0MQ socket
    message_client.bind("tcp://" + address)
    sleep(1) #Give tcp connection time to spin up

    name =  zk.create_master(address)
    print name
    return name

def on_new_job(zk_event):
    """
    watches for new jobs to be added to root
    if the job has no children (signaling they have already been assigned)
    then it pushes the jobs out to a worker in round robin fashion
    """
    try:
        for job in zk.get_jobs():
            try:
                if not zk.job_has_worker_assigned(job):
                    message_client.send(str(job))
            except NoNodeError:
                print "dang, someone beat me to it!"

    finally:
        zk.get_jobs(on_new_job)


# continuously loop until keyboard interrupt, on_new_job will fire every time a new job is added
if __name__ == "__main__":
    try:
        name = register(sys.argv[2])
        zk.get_jobs(on_new_job)
        print name
        while True:
            pass

    except (KeyboardInterrupt, SystemExit):
        print "exiting: " + name

    finally:
        zk.stop()
        message_client.close()
        context.term()
        sys.exit()
