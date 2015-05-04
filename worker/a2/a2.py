
class MsgHandler():
	def __init__(self, name):
		print("i am msg handler B")
		self.name = name

	def onMessage(self, msg):
		print("handler:", self.name, "message:", msg)

