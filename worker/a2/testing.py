
class MsgHandler():
	def __init__(self, name, context):
		print("i am msg handler B")
		self.name = name

	def onMessage(self, msg, context):
		print("handler:", self.name, "message:", msg)

