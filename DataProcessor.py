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

import zmq
import time
import pickle

# Prepare our context and publisher
context    = zmq.Context()
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://localhost:5556")
subscriber.RCVTIMEO = 10000
    

starttime = time.time()
messagecount = 0
print("DataProcessor connecting to DataBroker via PubSub...")

# Subscribe to all channels in range()
for i in range(0,numChannels):
    subscriber.setsockopt(zmq.SUBSCRIBE, str.encode("Channel" + "-" + str(i) + "-"))

print("Subscribed to " + str(i+1) + " Channels")
while True:
    # Read envelope with address
    try:        
        [address, contents] = subscriber.recv_multipart()
    except:
        print("Disconnected from DataBroker, exiting...")
        break
    messagedata = pickle.loads(contents)
    if address == str.encode("Channel" + "-" + str(1) + "-"):
        messagecount = messagecount + 1;
        if messagecount % int(round(5/blockSize*sampleFreq)) == 0:
            print("Recieved " + str(messagecount*blockSize) + " floats on Channel 1")
    
# Clean Up
subscriber.close()
context.term()   