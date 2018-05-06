#!/bin/bash
# Dummy jar file
#java -jar GenericNode.jar

#TCP Server -centralized node directory if used 
#java -jar GenericNode.jar ts 4410

#TCP Server -KV store
java -jar GenericNode.jar ts 1234 172.17.0.2 4410 3

#RMI Server
#rmiregistry -J-Djava.class.path=GenericNode.jar &
#java -Djava.rmi.server.codebase=file:GenericNode.jar -cp GenericNode.jar genericnode.GenericNode rmis 

