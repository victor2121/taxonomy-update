#!/bin/python
# -*-coding:utf-8 -*-

import logging
import threading
import time
import urllib
import string
import re
import sys
from md5 import *
import socket	

class Communicator(object):
	
	def __init__(self, args):
		self.args = args
		# fix the timeout
		socket.setdefaulttimeout(float(self.args["sock_timeout"]))
		logging.info("socket timeout fixed at {} seconds".format(self.args["sock_timeout"]))

	def reporthook(self, blocks_read, block_size, total_size):
		if not blocks_read:
			logging.debug('Connection opened')
			return
		if total_size < 0:
			# Unknown size
			print 'Read %d blocks' % blocks_read
		else:
			amount_read = blocks_read * block_size
			percent = (amount_read * 100) / total_size
			sys.stdout.write('\rdownloading ... %d/100' % percent)
			sys.stdout.flush()
		return
	
	def checking_preliminary_requirements(self):
		ncbi_sum = self.ask_NCBI(self.args["url"] + "/" + self.args["file_zip"] + ".md5")[:32]

		local_sum = calc_md5_sum(self.args["place"] + "/" + self.args["file_zip"])
		
		logging.info("NCBI sum = {}".format(ncbi_sum))
		logging.info("local sum = {}".format(local_sum))
		
		if compare_md5sum(ncbi_sum, local_sum) == 1:
			logging.debug("md5sums are equal, nothing to do")
		else:
			self.find_my_file()
	
	def ask_NCBI(self, url=None):
		try:
			response = urllib.urlopen(url if url is not None else self.args["url"])
			return response.read()
		except Exception, e:
			logging.fatal(e)
			sys.exit(2)
	
	def find_my_file(self):
		# asking NCBI
		lines = self.ask_NCBI(self.args["url"]).split('\n')
		
		# building pattern to searching
		name_pattern = re.compile(".*" + self.args["file_zip"] + "\s|$")
		
		# Searching
		for num,line in enumerate(lines):
			if name_pattern.match(line) and len(line) > 15:
				logging.info("found! line {} : {}".format(num,line))
				self.download_file()
				break
		else:
			logging.fatal("Nothing found")
			sys.exit(2)
	
	def download_file(self):
		try:
			urllib.urlretrieve(self.args["url"] + self.args["file_zip"],\
			self.args["place"] + "/" + self.args["file_zip"], reporthook=self.reporthook)
			print ''
			
		finally:
			urllib.urlcleanup()
			logging.info("Downloading is done - Connection closed")
			logging.info("Checking of integrality of {} ...".format(self.args["file_zip"]))
						
			# Last cheking after operations
			if compare_md5sum(self.ask_NCBI(\
			self.args["url"] + "/" + self.args["file_zip"] + ".md5")[:32],\
			 calc_md5_sum(self.args["place"] + "/" + self.args["file_zip"])) == 1:
				logging.info("The file status is satisfactory")
			else:
				logging.fatal("Error during the downloading process, please retry later.")
				sys.exit(2)

