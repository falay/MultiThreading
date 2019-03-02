import threading
import queue
import time
from multiprocessing.dummy import Pool

import sys


# A singleton for Job Queue Manager
class JobQueueManager(object):

	_single = None
	_jobQueue = {}

	def __init__(self):
		super( JobQueueManager, self ).__init__()

	@classmethod
	def __new__(self, clz):
		if not JobQueueManager._single:
			JobQueueManager._single = object.__new__(clz)
		return JobQueueManager._single

	def getJobQueue(self, name):
		if not JobQueueManager._jobQueue.get(name):
			JobQueueManager._jobQueue[name] = queue.Queue()
		return JobQueueManager._jobQueue[name]


# A singleton for background daemon manager
class BackgroundDaemonManager(object):

	_single = None
	_backgroundDaemons = []

	def __init__(self):
		super( BackgroundDaemonManager, self ).__init__()

	@classmethod
	def __new__(self, clz):
		if not BackgroundDaemonManager._single:
			BackgroundDaemonManager._single = object.__new__(clz)
		return BackgroundDaemonManager._single

	def addBackgroundDaemon(self, daemon):
		BackgroundDaemonManager._backgroundDaemons.append( daemon )


	def start(self):
		for daemon in BackgroundDaemonManager._backgroundDaemons:
			#daemon.setDaemon(True)
			daemon.dameon = True
			daemon.start()



class Daemon(threading.Thread):

	def __init__(self, name):
		super( Daemon, self ).__init__()
		self.name = name
		#BackgroundDaemonManager().addBackgroundDaemon( self )


class WorkerDaemon(Daemon):

	def __init__(self, name, my_queue, pool, wrap_func):
		super( WorkerDaemon, self ).__init__(name)
		self.queue = my_queue
		self.pool = pool
		self.wrap_func = wrap_func

	def run(self):
		time.sleep(1)
		while True:
			job_instance = self.queue.get()
			result = self.pool.apply_async( self.wrap_func, (job_instance,) )
			print(result.get())





def test_pool():

	thread_pool = Pool(4)
	results = [ thread_pool.apply_async(do_job, (job_id,)) for job_id in range(10) ]
	thread_pool.close()
	thread_pool.join()

	for res in results:
		print( res.get() )



def do_job(id):

	print("Done Job %d" % id)
	return id**2


def put_jobs(max_job):

	job_queue = JobQueueManager().getJobQueue('JobQueue')
	for job_id in range(max_job):
		job_queue.put( job_id )




def register_workerDaemon():

	daemon_manager = BackgroundDaemonManager()
	queue = JobQueueManager().getJobQueue('JobQueue')
	pool = Pool(4)
	daemon = WorkerDaemon('Worker_Daemon', queue, pool, do_job)
	daemon_manager.addBackgroundDaemon( daemon )


def wait_all_jobs():

	queue = JobQueueManager().getJobQueue('JobQueue')
	queue.join()



def TestCase1():
	
	# Initialize the worker daemon and start
	register_workerDaemon()
	daemon_manager = BackgroundDaemonManager()
	daemon_manager.start()

	# Simulation the incomming jobs
	put_jobs(15)

	time.sleep(3)

	put_jobs(100)

	time.sleep(1)

	put_jobs(207)

	#wait_all_jobs()
	
	#test_pool()


'''
***************** Another multithreading programming *****************
'''
class Job(object):
	def __init__(self, id):
		self.id = id


class MultiThreading:

	def __init__(self, num_of_worker_thread):

		self.jobQueue = queue.Queue()
		self.createWorkers( num_of_worker_thread )


	def createWorkers(self, num_of_worker_thread):

		for ID in range(num_of_worker_thread):
			worker = threading.Thread(target=self.process_job, args=(ID, self.jobQueue,))
			worker.daemon = True
			worker.start()

	def process_job(self, tid, jobQueue):

		while True:
			job = jobQueue.get()
			print( 'thread {} processing job {}'.format(tid, job.id) )
			jobQueue.task_done() # once this worker finished the job, it will say its task_done

	def putJobs(self, num_of_jobs):

		for job_id in range(num_of_jobs):
			job = Job(job_id)
			self.jobQueue.put( job )
		self.jobQueue.join() # join will wait all the worker's task_done



def TestCase2():

	MT = MultiThreading(10)
	
	#MT.putJobs(10000)
	time.sleep(2)
	MT.putJobs(15000)








if __name__ == '__main__':

	TestCase1()
	TestCase2()
