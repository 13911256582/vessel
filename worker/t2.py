
class MsgHandler():
	def __init__(self, name):
		self.name = name

	def onMessage(self, msg):
		print("handler:", self.name, "message:", msg)

