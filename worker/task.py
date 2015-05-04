
class MsgHandler():
	def __init__(self, name):
		print("this is master thread msg handler")
		self.name = name

	def onMessage(self, topic, msg, context):
		print("handler:", self.name, "topic:", topic, "message:", msg)

