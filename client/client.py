#connect host
#exit
#map source topics actor(name) --> "ok", uuid  or error
#load actor func(name) code(file name)  --> "ok"
#set actor {}
#get actor {}
#enable actor
#diable actor
#delete actor
#list

import redis
import Queue
import threading
import uuid
import os
import time
import json
import pdb


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


class ClientThread(RedisThread):
	def __init__(self, name, subChannels, context):
		RedisThread.__init__(self, name, subChannels, context)
		self.handler = self
		#self.callback = None

	def onMessage(self, topic, msg, context):
		print("-->", self.name, "topic:", topic, "message:", msg)

		client = context

		if topic == 'client':
			#broadcast from master
			return

		if topic == 'client/' + client.uuid:
			req = json.loads(msg)

			if req['type'] == 'response':
				print("-->", "response:", req['response'])
				client.queue.put(req['response'])
		else:
			return

	def sendResponse(self, sender, status):
		response = {"type": "response", "sender": sender, "response": status}
		print("<--", "topic:", self.context.publishChannel, "message:", response)
		print("\n\n")
		self.redis.publish(self.context.publishChannel, json.dumps(response))



class Actor():
	def __init__(self, name, uuid, source, topics):
		self.name = name
		self.uuid = uuid
		self.source = source
		self.topics = topics



#client have both clientID 
class Client():
	def __init__(self, userID):
		self.uuid = str(uuid.uuid4())
		self.userID = userID
		self.actors = {}
		self.alias = {}
		self.masterHost = ''
		self.masterChannel = ''
		self.subscribeChannels = ''
		self.publishChannel = ''
		self.queue = Queue.Queue()

	def sendMessage(self, channel, message):
		print("<--", "topic:", channel, "message:", message)
		self.redis.publish(channel, json.dumps(message))

	def getResponse(self, timeout):
		try:
			response = self.queue.get(True, timeout)
			return ('ok', response)
		except:
			return ('error: timeout', None)

	def connect(self, masterHost, masterChannel, subChannels):
		self.masterHost = masterHost
		self.redis = redis.StrictRedis()			#temp solution

		self.masterChannel = masterChannel
		self.subscribeChannels = subChannels
		self.publishChannel = masterChannel + '/' + self.uuid

		thread = ClientThread(self.uuid, subChannels, self)
		thread.start()

		#this is not a good solution, need to wait the thread to start
		time.sleep(1)

		self.masterThread = thread

		#notify master i am coming
		req = {"type": "request", "request": "connect", "userID": self.userID, "clientID": self.uuid}
		self.sendMessage(masterChannel, req)

	def exit(self):
		req = {"type": "request", "request": "exit", "clientID": self.uuid}
		self.sendMessage(masterChannel, req)
		self.thread.stop()

	def map(self, source, topics, actorName):
		req = {"type": "request",  "request": "map", "clientID": self.uuid, "source": source, "topics": topics, "actor": actorName}
		self.sendMessage(masterChannel, req)
		e, response = self.getResponse(1)
		if e == 'ok':
			uuid = self.response['actor']
			actor = Actor(actorName, uuid, source, topics)
			self.actors[uuid] = actor
			self.alias[actorName] = actor
			print("ok", actorName, uuid)
		else:
			print e

	def unmap(self, actorName):
		e, actor = self.getByName(actorName)
		if e == 'ok':
			return self._unmap(actor)
		else:
			return e

	def enable(self, actorName):
		pass

	def disable(self, actorName):
		pass


	def _unmap(self, actor):
		pass

	def _enable(self, actor):
		pass

	def _disable(self, actor):
		pass

	def getByID(self, actorID):
		if actorID in self.actors:
			return "ok", self.actors[actorID]
		else:
			return "error: actor not exist", None

	def getByName(self, actorName):
		if actorName in self.alias:
			return "ok", self.alias[actorName]
		else:
			return "error: actor not exist", None

	def displayActor(self, actor):
		print("name:", actor.name, "uuid:", actor.uuid, "source:", actor.source, "topics:", actor.topics)

	#list all mapping points(actors)
	def list(self):
		for actor in self.actors:
			self.displayActor(actor)


	def load(self, actor, func, code):
		print('actor:', actor, 'func:', func, 'code:', code)




if __name__ == "__main__":

	client = Client()
	while True:
		line = raw_input(">")

		words = line.split()
		cmd = words[0]
		
		if cmd == 'connect':
			if len(words) == 2:
				host = words[1]
				channels = ["client", "client/" + client.uuid]
				client.connect(host, 'master/client', channels)
			else:
				print("connect: connect host")
		
		elif cmd == 'exit':
			client.exit()
			break

		elif cmd == 'map':
			if len(words) == 4:
				source = words[1]
				topics = words[2].split(',')
				actor = words[3]
				client.map(source, topics, actor)
			else:
				print("map: map source 'topic1, topic2, etc.' actor_name ")

		elif cmd == 'load':
			if len(words) == 4:
				actor = words[1]
				func = words[2]
				code = words[3]
				client.load(actor, func, code)
			else:
				print("load: load actor_name func_name code_file")

		elif cmd == 'enable':
			if len(words) == 2:
				actor = words[1]
				client.enable(actor)
			else:
				print("enable: enable actor_name")

		elif cmd == 'disable':
			if len(words) == 2:
				actor = words[1]
				client.disable(actor)
			else:
				print("disable: disable actor_name")

		else:
			print("wrong command")



