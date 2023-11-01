# Runs a basic DHT server.



# ================================================================================================= Imports
import select
import socket
from queue import Queue
from threading import Thread
from typing import Callable

from bencode import bencode



# ================================================================================================= Global Variables
PORT:int = 6881
SOCK_RECV_BUF_SIZE:int = 1024



# ================================================================================================= Helper Structures
# An Address is a remote hostname or IP, and port.
class Address:
	def __init__(self, addr:str, port:int) -> None:
		self.addr:str = addr
		self.port:int = port


	def asTuple(self) -> tuple[str,int]:
		return (self.addr, self.port)


# A Message is a dict (that will be converted to a bencoded bytestring before sending) that is to
#   be sent out to a given Address.
class Message:
	def __init__(self, msg:dict, to:Address) -> None:
		self.msg:dict = msg
		self.to:Address = to


	def encoded(self) -> bytes:
		"""Returns the bencoded message, which is how DHT messages are required to be formatted."""
		return bencode(self.msg)



# ================================================================================================= Class DHTServer
class DHTServer:
	def __init__(self, logger:Callable|None=None) -> None:
		# Use a basic, built-in log function if none is provided.
		self.log:Callable = logger if logger else self._log

		# Set up a socket to listen for UDP packets on the configured port.
		self.sock:socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.bind(('0.0.0.0', PORT))

		# Initialise the server's state.
		self.running:bool = False  # Will be set to True once start() is called.
		self.comms_thread:Thread|None = None  # Holds the thread handle for the server's actual communications component.
		self.send_queue:Queue[Message] = Queue()  # A queue of messages to be sent out via UDP.

		# Create a recv/send socket pair. The send socket can signal the recv socket which will
		#   indicate that there is data ready to send out via the main socket.
		self.sig_recv:socket.socket;self.sig_send:socket.socket
		self.sig_recv, self.sig_send = socket.socketpair()


	def _log(self, message:str, level:str='info') -> None:
		"""A simple log function to be used when a more extensive logger is not in play."""
		print(f"[{level}] {message}")


	def commsLoop(self) -> None:
		"""Loops forever, listening for incoming UDP datagrams and processing them, and sending
			out queued messages."""

		while self.running:
			# Wait for either the main socket to receive data, or for the signal socket to receive
			#   data, which indicates that there is a Message available in the queue to send out.
			ready_sockets:list[socket.socket]
			ready_sockets, _, _ = select.select([self.sock, self.sig_recv], [], [])

			for rs in ready_sockets:
				# If the main socket has received data, process the incoming message.
				if rs is self.sock:
					data:bytes;addr:tuple
					data, addr = self.sock.recvfrom(SOCK_RECV_BUF_SIZE)

					# Log the received message for debug purposes.
					self.log(f"Received data from {addr}: {data}", 'debug')

					#TODO self.processMessage(bdecode(data))

				# Otherwise, if the signal socket has received data, that means there is a Message
				#   in the queue ready to send out.
				elif rs is self.sig_recv:
					self.sig_recv.recv(1)

					msg:Message = self.send_queue.get()
					self.log(f"Sending Message to {msg.to.asTuple()}: {msg.msg}", 'debug')
					self.sock.sendto(msg.encoded(), msg.to.asTuple())


	def start(self) -> None:
		"""Runs the main communications component of the DHT server in a new thread."""

		self.running = True

		self.comms_thread = Thread(target=self.commsLoop)
		self.comms_thread.start()
		self.log('Started DHT server communications thread.', 'info')


	def sendMessage(self, msg:Message) -> None:
		"""Adds the provided Message to the send queue and then sends a signal to process the
			queue."""

		self.send_queue.put(msg)

		# Send a single byte of data through the signal send socket to wake up the main
		#   communications thread, which will then process the send queue.
		self.sig_send.send(b'\x00')



#TODO: The following is temporary test code.
dht_serv:DHTServer = DHTServer()
dht_serv.start()
dht_serv.sendMessage(Message({'msg':'test'}, Address('127.0.0.1', 6881)))
