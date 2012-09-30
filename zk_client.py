from __builtin__ import super, staticmethod
from exceptions import SystemExit
import json
from kazoo.client import KazooClient
from kazoo.protocol.states import EventType

def halt_if_deleted(zk_event):
    """ callback for own node being deleted"""
    type, _, path = zk_event
    if type == EventType.DELETED:
        print "exiting due to znode deletion of: " + path
        raise SystemExit


class ZkClient(KazooClient):

    def __init__(self, address, root):
        super(ZkClient,self).__init__(address)
        self.root = root
        self.jobs_root = self.root + "/jobs"
        self.master_root = self.root + "/masters"
        self.workers_root = self.root + "/workers"
        self.start()

    def __del__(self):
        self.stop()

    def get_masters(self):
        return self.get_children(self.master_root)

    def get_master(self, master_name):
        return self.get("{0}/{1}".format(self.master_root, master_name))

    def get_jobs(self, watch = None):
        return self.get_children(self.jobs_root, watch= watch)

    def get_job(self, job):
        return self.get("{0}/{1}".format(self.jobs_root, job))

    def job_has_worker_assigned(self, job, watch = None):
        job_path = "{0}/{1}".format(self.jobs_root,job)
        return len(self.get_children(job_path, watch=watch)) > 0

    def delete_job(self, job_name):
        job_node = "{0}/{1}/".format(self.jobs_root, job_name)
        return self.delete(job_node, recursive = True)

    def mark_job(self, job_name, mark):
        job_node = "{0}/{1}/".format(self.jobs_root, job_name)
        in_progress_node_path = "{0}{1}".format(job_node, mark)
        return self.create(in_progress_node_path,ephemeral = True)

    def create_worker(self, master_name, on_master_deletion, on_deleted = halt_if_deleted):
        name =  self.create(
            "{0}/worker-".format(self.workers_root)
            ,value = json.dumps({"master" : master_name})
            ,ephemeral = True
            ,sequence = True)

        self.exists("{0}/{1}".format(self.master_root,master_name), watch = on_master_deletion)
        self.exists(name, watch = on_deleted)
        return name

    def create_master(self, listening_on, on_deleted = halt_if_deleted):
        path = "{0}/{1}".format(self.master_root,"master-")
        name =  self.create(path, ephemeral= True, sequence=True, value= json.dumps({ "listening_on" : listening_on }))
        self.exists(name, watch = on_deleted)
        return name



