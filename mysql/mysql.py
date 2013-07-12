#!/bin/python
# -*-coding:utf-8 -*-


import MySQLdb as db
from MySQLdb.cursors import Cursor
import sql_builder as sqlbuild
import logging
import threading
import re
import time
import sys
from types import ListType, TupleType, UnicodeType


restr = (r"\svalues\s*"
        r"(\(((?<!\\)'[^\)]*?\)[^\)]*(?<!\\)?'"
        r"|[^\(\)]|"
        r"(?:\([^\)]*\))"
        r")+\))")

insert_values= re.compile(restr)
from _mysql_exceptions import Warning, Error, InterfaceError, DataError, \
     DatabaseError, OperationalError, IntegrityError, InternalError, \
     NotSupportedError, ProgrammingError

class cursor_perso(Cursor):
	
	
	def __init__(self, connection):
		self.thread_delay = 0.0
		Cursor.__init__(self, connection)
	
	def executemany(self, query, args):
		del self.messages[:]
		db = self._get_db()
		if not args: return
		charset = db.character_set_name()
		if isinstance(query, unicode): query = query.encode(charset)
		m = insert_values.search(query)
		if not m:
			r = 0
			for a in args:
				time.sleep(self.thread_delay) # for perf
				r = r + self.execute(query, a)
			return r
		p = m.start(1)
		e = m.end(1)
		qv = m.group(1)
		try:
			q = [ qv % db.literal(a) for a in args ]
		except TypeError, msg:
			if msg.args[0] in ("not enough arguments for format string",
							   "not all arguments converted"):
				self.errorhandler(self, ProgrammingError, msg.args[0])
			else:
				self.errorhandler(self, TypeError, msg)
		except:
			exc, value, tb = sys.exc_info()
			del tb
			self.errorhandler(self, exc, value)
		r = self._query('\n'.join([query[:p], ',\n'.join(q), query[e:]]))
		if not self._defer_warnings: self._warning_check()
		return r
		

class Communicator(object):
	
	def __init__(self, mysql_username, mysql_password, mysql_database="taxonomy", mysql_host="localhost", mysql_block_size=10000, td=0.0):
		self.host = mysql_host
		self.username = mysql_username
		self.password = mysql_password
		self.database = mysql_database
		self.maximum = int(mysql_block_size)
		self.offset = 0
		self.td = td
		logging.debug("new communicator asked : username:{}; host:{};database:{}; bs:{}".format(self.username, self.host, self.database, self.maximum))
	
	def __repr__(self):
		return "username : {username}, host : {host}, database : {database}, bs : {maximum}, td : {td} "\
		.format(username = self.username,\
		host = self.host, database = self.database,\
		maximum = self.maximum, td = self.td)
		
	def connect(self):
		logging.debug("New connection asked") 
		try:
			return db.connect(self.host, self.username, self.password, self.database)
		except db.Error, e:
			logging.fatal("Error {}: {}".format(e.args[0], e.args[1]))
			sys.exit(56)

	def create_table(self, name, *args):
		
		con = self.connect()
		
		if con:
			try:
				with con:
					cur = con.cursor()
					cur.execute("DROP TABLE IF EXISTS " + name)
					
					query = sqlbuild.build_sql_createtable(name, args)
					cur.execute(query)
			except db.Error, e:
				logging.error("Error {}: {}".format(e.args[0], e.args[1]))
				return None
			finally:
				if con:
					con.close()

	def alter_table(self, name, *args):
		con = self.connect()
		
		if con:
			try:
				with con:
					cur = con.cursor()
					
					query = sqlbuild.build_sql_altertable(name, args)

					cur.execute(query)
			except db.Error, e:
				logging.error("Error {}: {}".format(e.args[0], e.args[1]))
				return None
			finally:
				if con:
					con.close()
		
		
		
	def insert(self, name, **kwargs):
		con = self.connect()
		
		if con:
			try:
				with con:
					cur = con.cursor()
					query = sqlbuild.build_sql_insert(name, kwargs)
					
					cur.execute(query)
					
			except db.Error, e:
				logging.error("Error {}: {}".format(e.args[0], e.args[1]))
				return None
			finally:
				if con:
					con.close()
					
	def _iterator_sequencor(self, values, bs=-1):
		
		if bs == -1:
			bs = self.maximum
		
		new_list = []
		size_values = len(values)
		counter = bs * self.offset
		while counter < size_values:
			while counter < size_values:
				new_list.append(values[counter])
				counter += 1
				if len(new_list) == self.maximum:
					break
			yield new_list
			new_list = []
		
	def insertmany(self, name, values, **keys):
		
		
		total_iter = len(values)
		
		print total_iter
		
		if len(values) >= self.maximum:
			sequencor = self._iterator_sequencor(values)
		else:
			sequencor = self._iterator_sequencor(values, bs=total_iter)
		
		new_keys = {}
		sorted_values = keys.values()
		sorted_values.sort()
		
		searchkey = lambda d, val: [c for c,v in d.items() if v==val]
		
		for k in sorted_values:
			new_keys[k] = searchkey(keys, k)[0]
			
		kwargs = [(v,'%s') for k, v in new_keys.items()]
		
		con = self.connect()
		
		if con:
			try:
				with con:
					cur = con.cursor(cursorclass=cursor_perso)
					cur.thread_delay = self.td
					query = sqlbuild.build_sql_insert(name, kwargs)
					logging.info(query)
					
					i = 0

					logging.info("in transfering ...")
					percent = 0
					for seq in sequencor:
						
						cur.executemany(query, seq)
						
						if total_iter >= self.maximum:
							i += self.maximum
						else:
							i = total_iter

						percent = (i * 100) / total_iter
						
						sys.stdout.write('\r%d/100' % (percent))
						sys.stdout.flush()
						
						logging.debug("commit, offset {} (bs={})".format((i / self.maximum) , self.maximum))
						
						con.commit()
						
					sys.stdout.write('\r\n')
					sys.stdout.flush()
			except db.Error, e:
				logging.fatal("Error {}: {}".format(e.args[0], e.args[1]))
				sys.exit(67)
			finally:
				if con:
					con.close()
				
	def retrieve_all(self, name, *args):
		con = self.connect()
		
		if con:
			try:
				with con:
					cur = con.cursor()
					query = sqlbuild.build_sql_select(name, args)
					cur.execute(query)
					return cur.fetchall()
			except db.Error, e:
				logging.fatal("Error {}: {}".format(e.args[0], e.args[1]))
				sys.exit(67)
			finally:
				if con:
					con.close()

