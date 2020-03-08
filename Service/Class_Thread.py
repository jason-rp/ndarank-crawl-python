def Exit():
    print "Processing 5 Secs To Close"
    try:
        import time as taomuon
        taomuon.sleep(5)
    except:
        pass
    try:
        import sys
        sys.exit()
    except:
        raise SystemExit
    pass
pass
try:
	import queue
except:
    try:
	    import Queue as queue
    except:
        print "Import Module Error Class_Thread [Queue_Not_Found]"
        Exit()
        pass
    pass
pass
try:
    import threading
except:
    print "Import Module Error Class_Thread [Thread_Not_Found]"
    Exit()
pass

class Threader:
	def __init__(self,pool_size=1):
		self.q = queue.Queue()
		self.pool_size = pool_size
	def __t(self):
		thread = self.q.get()
		thread.daemon=True
		thread.start()
	def __name(self):
		return "Groupon_THREAD"

	def __wait(self):
		while 1:
			running = threading.enumerate()
			remain = [x.name for x in running if self.__name() in x.name]
			if len(remain) == 0:
				break
        pass

	def on_waiting(self):
		return self.q.qsize()

	def pop(self):
		return self.q.queue.pop()

	def finish_all(self):
		for _ in xrange(self.q.qsize()): self.__t()
		self.__wait()


	def put(self,target,args):
		if self.q.qsize() < self.pool_size:
			self.q.put(threading.Thread(target=target,name=self.__name(),args=tuple(args)))

		if self.q.qsize() >= self.pool_size:
			for _ in xrange(self.q.qsize()): self.__t()
			self.__wait()
        pass