import redis
import threading
import uuid
import time
import json
import pdb
import Queue #v3 import queue

class RedisThread(threading.Thread):
	def __init__(self, name, topics, context):
		threading.Thread.__init__(self)
		self.topics = topics
		self.name = name
		self.context = context
		self.thread_stop = False
		self.callback = None
		self.redis = None


	def run(self):
		self.redis = self.context.redis
		p = self.redis.pubsub()
		p.subscribe(self.topics)
		for msg in p.listen():
			if msg['data'] == 'exit' or self.thread_stop == True:
				print("Thread:", self.name, "exit")
				p.unsubscribe(self.topics)
				p.close()
				return
			else:
				if msg['type'] == 'message':
					self.onMessage(msg, self.context)
				else:
					print(msg)


	def onMessage(self, message, context):
		pass

	def stop(self):
		self.thread_stop = True

class WorkerThread(RedisThread):
	def __init__(self, name, topics, context):
		RedisThread.__init__(self, name, topics, context)
		self.queues = {}

	def addQueue(self, sender):
		if sender in self.queues:
			return
		else:
			q = Queue.Queue()
			self.queues[sender] = q

	def delQueue(self, sender):
		if sender in self.queues:
			del self.queues[sender]


	def onMessage(self, message, context):
		topic = message['channel']
		msg = json.loads(message['data'])
		worker = context

		if topic == 'master/worker/' + worker.uuid:
			if msg['type'] == 'response':
				print("-->", "response:", msg['response'])
				print("\n")
				sender = msg['sender']
				if sender in self.queues:
					self.queues[sender].put(msg)	#this will wake-up the client who is waiting for the response
				else:
					pass
			else:
				#currently there is no other message with type!=response
				return
		else:
			return

	def stop(self):
		self.thread_stop = True



class MasterThread(RedisThread):
	def __init__(self, context):
		RedisThread.__init__(self, "master", ["master/worker", "master/client"], context)

	def onMessage(self, message, context):
		if message['type'] == 'message' and message['channel'] == "master/worker":
			self.handlWorker(message['data'])
		elif message['type'] == 'message' and message['channel'] == "master/client":
			self.handleClient(message['data'])
		else:
			print(message)

	def handlWorker(self, msg):
		#{"request":"register", "type":"redis", "workerID": uuid}
		data = json.loads(msg)
		if data['request'] == 'register':
			master = self.context
			#convert unicode to str
			master.register(data['type'], data['workerID'])

	def handleClient(self, data):
		pass

class Source():
	pass


#master and worker communication has three channels:
#channel1: master/worker, master listen (a dedicate thread) to this channel for incoming request from worker
#channel2: worker/[123], master send message to work[123] through this channel
#channel3: master/worker/[123], master get_message from this channel for response, no thread waiting, synchrounous request/response

class Worker():
	def __init__(self, name, workerType, workerID):
		self.type = workerType
		self.name = name 			#name of this worker, could be an uuid
		self.uuid = workerID		#this is the uuid of the remoter worker
		self.actors = {}
		self.subChannel = "master/worker/" + self.uuid
		self.pubChannel = "worker/" + self.uuid
		self.thread = None
		self.redis = None

	def run(self):
		self.redis = redis.StrictRedis()
		thread = WorkerThread(self.name, ["master/worker", self.subChannel], self)
		thread.start()
		self.thread = thread

	def stop(self):
		if self.redis:
			p = self.redis.pubsub()
			p.unsubscribe(self.subChannel)


	def createActor(self, sender, topics, name):
		req = {"request": "create", "topics": topics, "actor": name}
		# we need to make sure message is in order!!!
	
		ret, response = self._doRequest(sender, req)

		if ret == 'ok':
			actor = Actor(name, name)
			self.actors[actor.uuid] = actor
			return ("ok", actor)
		else:
			return (ret, None)

	def loadActor(self, sender, actor, func, code):
		if actor.uuid in self.actors:
			req = {"request": "load", "actor": actor.uuid, "func": func, "code": code}
			return self._doRequest(sender, req)
		else:
			return "error: actor {" + actor.name + "} does not exist" 


	def enableActor(self, sender, actor):
		if actor.uuid in self.actors:
			req = {"request": "enable", "actor": actor.uuid}
			return self._doRequest(sender, req)
		else:
			return "error: actor {" + actor.name + "} does not exist" 

	def disableActor(self, actor):
		pass

	def setActor(self, actor):
		pass

	def getActor(self, actor):
		pass

	def deleteActor(self, actor):
		pass

	#send message to worker
	def _sendMessage(self, sender, message):
		print("<--", "channel:", self.pubChannel, "message:", message)

		self.redis.publish(self.pubChannel, json.dumps(message))

	def _getMessage(self, sender, timeout):
		#assume queue has been established between worker and workerThread
		q = self.thread.queues[sender]
		#block, waiting for response
		try:
			response = q.get(True, timeout)
			return ('ok', response)
		except:
			return ('error: timeout', None)

	#send request to remote worker, and wait for response
	#sender is used to identify who is sending this message, because this function allow parallelly run
	def _doRequest(self, sender, request):
		request['sender'] = sender
		request['type'] = 'request'
		self._sendMessage(sender, request)
		return self._getMessage(sender, timeout = 1)

	def list(self):
		pass



class Actor():
	def __init__(self, name, uuid):
		self.name = name
		self.uuid = uuid

class Client():
	def map(self, source, topics, actor):
		pass

	def load(self, actor, code):
		pass

	def set(self, actor, setting):
		pass

	def get(self, actor):
		pass

	def enable(self, actor):
		pass

	def disable(self, actor):
		pass

	def delete(self, actor):
		pass



class Master():
	def __init__(self):
		self.workers = {}
		self.clients = {}
		self.sources = {}

	def run(self):
		self.redis = redis.StrictRedis()
		thread = MasterThread(self)
		thread.start()

	#admin
	def getWorker(self, worker):
		pass

	def listWorkers(self):
		pass

	def listClients(self):
		pass


	#worker side interfaces
	#worker -> master
	def register(self, workerType, workerID):
		if not workerID in self.workers:
			worker = Worker("worker/1", workerType, workerID)
			if worker:
				self.workers[workerID] = worker
				worker.run()
				msg = {"type" : "response", "response":"ok"}

				self.notify(worker.pubChannel, msg)
				#worker._sendMessage(msg)
				print("worker registered:", workerID)
			else:
				e = "error: register failed"
				msg = {"type": "response", "response":e}
				self.notify(worker.pubChannel, msg)
		else:
			e = "error: worker exist "
			msg = {"type": "response", "response":e}
			self.notify(worker.pubChannel, msg)

	def pickWorker(self, workerType):
		for worker in self.workers:
			if self.workers[worker].type == workerType:
				return self.workers[worker]
		return None

	def notify(self, channel, message):
		print("<--", "channel:", channel, "message:", message)
		self.redis.publish(channel, json.dumps(message))

	#client side interfaces
	def connect(self, client):
		pass

	def close(self, client):
		pass

	def list(self, client):
		pass

	def map(self, client, source, topics, actor):
		if self.clients[client]:
			ret = self.clients[client].map(source, topics, actor)
			return ret
		else:
			return "error: client not exist"

	def load(self, client, actor, code):
		pass

	def set(self, client, actor, setting):
		pass

	def get(self, client, actor):
		pass

	def enable(self, client, actor):
		pass

	def disable(self, client, actor):
		pass

	def delete(self, client, actor):
		pass


def loadCode(name):
	code = ''
	f = open(name, 'r')
	for line in f:
		code = code + line
	return code

if __name__ == "__main__":
	master = Master()
	master.run()


	while True:
		worker = master.pickWorker("redis")
		if worker:
			break
		else:
			time.sleep(1)

	sender = "client123"
	worker.thread.addQueue(sender)				#a queue for each client session
	error, actor = worker.createActor(sender, ['worker', 'worker/air_sensor'], 'air_sensor')

	if error == 'ok':
		code = loadCode("task.py")
		ret, response = worker.loadActor(sender, actor, "task", code)
		if ret == 'ok':
			worker.enableActor(sender, actor)
		else:
			print response
	else:
		print(error)




