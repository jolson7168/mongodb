from __future__ import print_function
import glob
import os
import json
import csv
import logging
from collections import defaultdict
import sys
import getopt
from time import gmtime, strftime
import decimal
import pika
import pymongo

config = {}
logger = logging.getLogger("GlobalLog")
overrides=defaultdict(dict)



def initLog():
	logger = logging.getLogger(config["logname"])
	hdlr = logging.FileHandler(config["logFile"])
	formatter = logging.Formatter(config["logFormat"],config["logTimeFormat"])
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr) 
	logger.setLevel(logging.INFO)
	return logger

def processFile(path,collection):
	logger.info("Processing: "+path)
	with open(path) as itemFile: 
    		aItem = json.loads(itemFile)
		collection.insert(aItem)

def processQueueItem(ch, method, properties, body):
	    aItem = json.loads(body)
	    print("Processing: "+aItem["_id"])
	    processItem(aItem,overrides)
	    fpath=str(config["outputFilePath"]+"/"+aPart["_id"]+".json".replace(" ","\ ").encode('ascii','ignore'))
	    print("Path: "+fpath)

def main(argv):
 	try:
      		opts, args = getopt.getopt(argv,"hc:",["configfile="])
	except getopt.GetoptError:
		print ('bulkfileloader.py -c <configfile>')
      		sys.exit(2)
	for opt, arg in opts:
      		if opt == '-h':
         		print ('bulkfileloader.py -c <configfile>')
         		sys.exit()
		elif opt in ("-c", "--configfile"):
			configFile=arg
			try:
   				with open(configFile): pass
			except IOError:
   				print ('Configuration file: '+configFile+' not found')
				sys.exit(2)
	execfile(configFile, config)
	logger=initLog()
	logger.info('Starting Run  ========================================')
	client = pymongo.MongoClient(config["mongoDBip"], config["mongoDBport"])
	db = client[config["mongoDBdatabase"]]
	collection = db[config["mongoDBcollection"]]
	if (config["inputType"] == 'file'):
		os.chdir(config["inputDirectory"])
		count=0
		for jsonFiles in glob.glob("*.json"):
			processFile(config["inputDirectory"]+'/'+jsonFiles,collection)
			count = count+1
	else:
		connection = pika.BlockingConnection(pika.ConnectionParameters(host=config["inputQueueServerIp"]))
		channel = connection.channel()
		channel.queue_declare(queue=config["inputQueueName"])
		channel.basic_consume(processQueueItem,queue=config["inputQueueName"],no_ack=True)  
		channel.start_consuming()
	logger.info('Done!  Files processed: '+str(count)+' ========================================')

if __name__ == "__main__":
	main(sys.argv[1:])
