
class MsgHandler():
	def __init__(self, name):
		print("msg handler thread name:", name)
		self.name = name

	def onMessage(self, topic, msg, context):
		print("handler:", self.name, "topic:", topic, "message:", msg)

