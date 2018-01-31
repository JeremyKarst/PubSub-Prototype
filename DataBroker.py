# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 2018
@author: Jeremy Karst, Asymmetric Technologies

This Python program is part of three components that serve as a prototype for 
networking and data proliferation through DAS software.

The DataGenerator generates random float values at a specified frequency and
for the specified nubmer of channels. This data is saved to disk and also sent
to the DataBroker over TCP in pickled chunks of numChannels*blockSize

The DataBroker will accept TCP connections from the DataGenerator, unpickle its
data stream, and then split the data in to its appropriate channels. Each 
channel is separately pickled and published using ZMQ PubSub.

The DataProcessor subscribes to the specified channels (all by default), and
checks their topic matching against a single channel number for diagnostics.
The message contents are unpickled and available for use.

These programs may be started in any order, but must be started within 
socket.RCVTIMEO ms to avoid timeout. DataGenerator will terminate after 5 
minutes to avoid filling the host hard drive with random data files. Its 
termination will cause the other two programs to timeout and exit.
"""

""" ------------CONFIGURATION------------ """
sampleFreq = 843 # Simulated sample frequency of DAS
numChannels = 4000 # Number of channels to generate
#blockSize = 256 # Number of samples to package for tranmission, 64 matches NRL
messagesPerSecond = 5
blockSize = int(round(sampleFreq/messagesPerSecond))
"""---------------------------------------"""

import zmq # used for messaging
import numpy # easy multidimensional arrays and random number generation
import time # used for limiting runtime
import pickle # used for serialization of numpy arrays

# Prepare publisher and data reciept socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555") # Recieve data over TCP port 5555
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5556") # Publish data over port 5556
publisher.RCVTIMEO = 10000
socket.RCVTIMEO = 10000
socket.setsockopt(zmq.LINGER, 0)

messageNumber = 1
starttime = time.time()
message = numpy.random.rand(numChannels,blockSize)
print("DataBroker connecting to DataGenerator via TCP...")
print("DataBroker connecting to DataProcessor via PubSub...")
while True:
    #  Wait for next request from client
    try:
        message = pickle.loads(socket.recv())
    except:
        print("Disconnected from DataGenerator, exiting...")
        break
    if messageNumber % int(round(5/blockSize*sampleFreq)) == 0:
        print("Received ",messageNumber," message blocks")
    messageNumber = messageNumber + 1
    socket.send(b"ack")
    
    for i in range(0, numChannels):
        publisher.send_multipart([str.encode("Channel" + "-" + str(i) + "-"), pickle.dumps(message[i,:])])

# Clean Up
socket.close()
publisher.close()
context.term()