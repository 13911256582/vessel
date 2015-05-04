import json


class MsgHandler():
	def __init__(self, name):
		print("this is master thread msg handler")
		self.name = name

	def onMessage(self, topic, msg, context):
		print("handler:", self.name, "topic:", topic, "message:", msg)

		worker = context

		if topic == 'worker':
			#broadcast from master
			return

		if topic == 'worker/' + str(worker.uuid):
			req = json.loads(msg)
			if req['request'] == 'create':
				worker.create()
				return

			if req['load'] == 'load':
				worker.load()
				return

			if req['enable'] == 'enable':
				worker.enable()
				return
		else:
			return


