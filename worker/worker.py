
import redis
import threading
import uuid
import os
import time
import json
import pdb

class Actor():
	def __init__(self, topics, uuid):
		self.topics = topics
		self.uuid = uuid
		self.path = None
		self.module = None
		self.thread = None
 
		self.enable = False



class RedisThread(threading.Thread):
	def __init__(self, name, topics, context):
		threading.Thread.__init__(self)
		self.topics = topics
		self.name = name
		self.context = context
		self.handler = None
		self.thread_stop = False
		self.redis = None

	def run(self):
		self.redis = redis.StrictRedis()
		p = self.redis.pubsub()
		p.subscribe(self.topics)

		for msg in p.listen():
			if msg['data'] == 'exit' or self.thread_stop == True:
				print("Thread:", self.name, "exit")
				p.unsubscribe(self.topics)
				p.close()
				return

			if msg['type'] == 'message' and self.handler:
				self.handler.onMessage(msg['channel'], msg['data'], self.context)


	def loadModule(self, module):
		#module = self.name + '.' + self.name
		_temp = __import__(module, globals(), locals(), ['MsgHandler'], 0 )
		self.handler = _temp.MsgHandler(self.name)

	def stop(self):
		self.thread_stop = True


class WorkerThread(RedisThread):
	def __init__(self, name, subChannels, context):
		RedisThread.__init__(self, name, subChannels, context)
		self.handler = self
		self.callback = None

	def onMessage(self, topic, msg, context):
		print("-->", self.name, "topic:", topic, "message:", msg)

		worker = context

		if topic == 'worker':
			#broadcast from master
			return

		if topic == 'worker/' + worker.uuid:
			req = json.loads(msg)

			if req['type'] == 'response':
				print("-->", "response:", req['response'])

			elif req['type'] == 'request':

				if req['request'] == 'create':
					e = worker.create(req['topics'], req['actor'])
					self.sendStatus(req['sender'], e)
					return

				elif req['request'] == 'load':
					e = worker.load(req['actor'], req['func'], req['code'])
					self.sendStatus(req['sender'], e)
					return

				elif req['request'] == 'enable':
					e = worker.enable(req['actor'])
					self.sendStatus(req['sender'], e)
					return
		else:
			return

	def sendStatus(self, sender, status):
		msg = {"type": "response", "sender": sender, "response": status}
		self.sendResponse(msg)

	def sendResponse(self, response):
		print("<--", "topic:", self.context.publishChannel, "message:", response)
		print("\n\n")
		self.redis.publish(self.context.publishChannel, json.dumps(response))

class Worker():
	def __init__(self):
		#self.uuid = uuid.uuid4()
		self.uuid = "redis"
		self.redis = redis.StrictRedis()
		self.actors = {}
		self.masterHost = ''
		self.masterChannel = ''
		self.subscribeChannels = ''
		self.publishChannel = ''

	def connect(self, masterHost, masterChannel, subChannels):
		self.masterHost = masterHost
		self.masterChannel = masterChannel
		self.subscribeChannels = subChannels
		self.publishChannel = masterChannel + '/' + self.uuid

		thread = WorkerThread("redis", subChannels, self)
		thread.start()

		#this is not a good solution, need to wait the thread to start
		time.sleep(1)

		self.masterThread = thread

		#notify master i am coming
		self.sendMessage(masterChannel, {"workerID": str(self.uuid), "type": "redis", "request":"register"})


	def sendMessage(self, channel, message):
		print("<--", "topic:", channel, "message:", message)
		self.redis.publish(channel, json.dumps(message))
		



	def create(self, topics, name):
		#uuid = uuid.uuid4()
		uuid = name
		actor = Actor(topics, uuid)
		if actor:
			self.actors[uuid] = actor			
			return 'ok'
		else:
			return 'error: create actor [' + name + '] failed'

	def load(self, uuid, func, code):
		if uuid in self.actors:
			actor = self.actors[uuid]
			#try:
			self._createDir(actor)

			self._save(actor.path + func + ".py", code)
			actor.module = uuid + "." + func
			return "ok"
			#except:
			#	e = 'error: load actor [' + uuid + '] failed'
			#	return e
		else:
			e = 'error: actor does not exist'
			return e

	def _createDir(self, actor):
		#create new direct by using uuid
		path = actor.uuid + '/'
		if not os.path.exists(path):
			os.mkdir(path)

		if not os.path.exists(path + '__init__.py'):
			#create an empty file called __init__.py
			self._save(path + "__init__.py", "#")

		actor.path = path		

	def _save(self, filename, code):
		try:
			f = open(filename, 'w')
			f.write(code)
		finally:
			f.close()


	def set(self, uuid, settings):
		pass


	def enable(self, uuid):
		if uuid in self.actors:
			actor = self.actors[uuid]
			if actor.thread == None:
				thread = RedisThread(actor.uuid, actor.topics, self)
				thread.loadModule(actor.module)
				thread.start()
				actor.thread = thread
				actor.enable = True
				return "ok"
			else:
				return "error: thread already running"

	def disable(self, uuid):
		if uuid in self.actors:
			actor = self.actors[uuid]
			if actor.enable == True and actor.thread:
				actor.thread.stop()
				self.publish(actor.uuid, "exit")
				actor.thread = None
				actor.enable = False
				return "ok"
			else:
				return "error: thread not running"

	def publish(self, topic, message):
		self.redis.publish(topic, message)

	def subscribe(self, topic):
		p = self.redis.pubsub()
		p.subscribe(topic)		

	def unsubscribe(self, topic):
		
		p = self.redis.pubsub()
		p.unsubscribe(topic)


def testCreate(worker):
	actor = worker.create("redis", ["air_sensor"], "air_sensor")

	code = ''
	f = open("task.py", 'r')
	for line in f:
		code = code + line

	worker.load("air_sensor", code)

	ret = worker.enable(actor.uuid)

	print(ret)

if __name__ == "__main__":
	worker = Worker()

	channels = ["worker", "worker/" + worker.uuid]
	#subscribe to two topics. worker is a broadcast channel, worker/uuid is a 1-1 channel
	worker.connect("localhost", "master/worker", channels)

	






	


