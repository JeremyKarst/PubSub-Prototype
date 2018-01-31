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
runtime = 2 # Number of minutes to generate data
"""---------------------------------------"""

import zmq # used for messaging
import numpy # easy multidimensional arrays and random number generation
import time # used for limiting runtime
import pickle # used for serialization of numpy arrays
import os # allows file deletion
import csv

context = zmq.Context()

# Set up for writing to file
filename = "DataGenerator"
filenumber = 0
file = open(filename + str(filenumber) + ".deleteme","wb+")

# Socket to talk to DataBroker
print("DataGenerator connecting to DataBroker via TCP...")
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")
socket.RCVTIMEO = 10000 # Timeout duration in ms
socket.setsockopt(zmq.LINGER, 5)

loopcounter = 1
timetosleep = blockSize/sampleFreq
starttime = time.time()
loadlist = []
# Generate data and push to server
print("Generating " + str(sampleFreq*numChannels/1000000) + " million floats per second")
while True:
    cycletime = time.time()
    #if loopcounter % int(round(250*256/blockSize)) == 0: # Create new files
    #    file.close()
    #    try:
    #        os.remove(filename + str(filenumber) + ".deleteme")
    #    except:
    #        print(filename + str(filenumber) + ".deleteme" + " is held by another process, cannot delete!")
    #    filenumber = filenumber + 1
    #    file = open(filename + str(filenumber) + ".deleteme","wb+")
        
    if time.time()-starttime > 60*runtime:
        print("Reached ",runtime," Minute Runtime Limit, terminating.")
        break
    floats = numpy.random.rand(numChannels,blockSize)
    if loopcounter % int(round(5/blockSize*sampleFreq)) == 0: # Only give updates every 5 seconds
        print("Sent",numChannels*blockSize/1000000*loopcounter,"M Values in ",loopcounter," message blocks.")
        print("Percent of full load: ",(blockSize/sampleFreq - timetosleep)/(blockSize/sampleFreq)*100)
    #pickle.dump(floats,file) # Write out pickled data
    try:
        socket.send(pickle.dumps(floats)) # Send pickled data over socket
        ack = socket.recv() # Recieve acklowledgement for sent data
    except:
        print("Disconnected from DataBroker, exiting...")
        break
    loopcounter = loopcounter + 1
    timetosleep = blockSize/sampleFreq - (time.time() - cycletime)
    loadlist.append((blockSize/sampleFreq - timetosleep)/(blockSize/sampleFreq)*100)
    if timetosleep > 0:
        time.sleep(timetosleep) # Sleep to simulate sample rate

# Clean up after reaching runtime limit
file.close()
with open("sleepstats.csv", 'w+') as file:
    writer = csv.writer(file)
    writer.writerow(loadlist)
socket.close()
context.term()
file.close()
try:
    os.remove(filename + str(filenumber) + ".deleteme")
except:
    print(filename + str(filenumber) + ".deleteme" + " is held by another process, cannot delete!")