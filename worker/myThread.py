import threading
import redis
import time

class myThread(threading.Thread):
	def __init__(self, name, topic):
		threading.Thread.__init__(self)
		self.redis = redis.StrictRedis()
		self.topic = topic
		self.name = name
		self.thread_stop = False

		module = self.name + '.' + self.name
		_temp = __import__(module, globals(), locals(), ['MsgHandler'], 0 )
		self.handler = _temp.MsgHandler(self.name)

	def run(self):
		p = self.redis.pubsub()
		p.subscribe(self.topic)
		for msg in p.listen():
			if self.handler:
				self.handler.onMessage(msg['data'])
			if msg['data'] == 'exit':
				print("Thread:", self.name, "exit")
				return
			if self.thread_stop == True:
				print("Thread:", self.name, "return")
				return

	def stop(self):
		self.thread_stop = True


if __name__ == '__main__':
	thread1 = myThread("a1", "master")
	thread2 = myThread("a2", "master")
	thread1.start()
	thread2.start()

	time.sleep(10)
	thread1.stop()
	thread2.stop()


