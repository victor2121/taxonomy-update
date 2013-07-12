#/bin/python
#-*- coding:utf-8 -*-
from zip_module import *
import web.web as web
import argparse
import logging
from mysql import mysql_patterns, mysql
import sys
import time
import os

class Taxon(object):
	def __init__(self, ncbi_code, id_parent = None, id_rank = None, scientific_name = None):
		self.code = ncbi_code
		self.parent = id_parent
		self.rank = id_rank
		self.sname = scientific_name
		
def read_a_file(filename, args):
	return read_files_inside_zipfile(args["place"] + "/" + args["file_zip"], filename)
	
def retrieve_properties(prop_file):
	dicoval={}
	with open(prop_file,'rb') as path:
		lignes = path.readlines()
		for lig in lignes:
			## Deleting commentaries
			sp = lig.split('#')[0] 
			sp = sp.replace('\n' ,'')
			sp = sp.split('=')
			if len(sp)==2:
				dicoval[sp[0]] = sp[1]
	return dicoval

def configure_logger(file_log):
	logging.basicConfig(format='%(asctime)s - [%(levelname)s] - [%(filename)s/%(funcName)s:%(lineno)d] - %(message)s',\
	 level=logging.DEBUG,\
	  filename=file_log)
	
	logging.info("Logger configured.")
	
def download_it(args, force):
	## Downloading file
	logging.debug("calling instance of web communicator ...")
	commweb = web.Communicator(args)
	if force:
		commweb.find_my_file()
	else:
		logging.debug("calling checking_preliminary_requirements()")
		commweb.checking_preliminary_requirements()
	return

map_connection = {}
def connection_manager(**kwargs):
	try:
		return map_connection[frozenset(kwargs.items())]
	except:
		logging.info("New communicator instancied")
		map_connection[frozenset(kwargs.items())] = mysql.Communicator(**kwargs)
		return map_connection[frozenset(kwargs.items())]
		
def manage_ranktable(fnodes, properties):
	# generate a list ranks
	ranks = [l.split('|')[2].strip() for l in fnodes if len(l) > 3]
	# unique and sorting for ranks list
	new_ranks_list = list(set(ranks))
	new_ranks_list.sort()
	# Write the rank table
	comm = connection_manager(**properties_part("mysql_", properties))
	comm.insertmany("RANK", new_ranks_list, NAME=1)
	return

def manage_taxontable(fnodes, fnames, properties, offset):
	## READING DATABASE
	comm = connection_manager(**properties_part("mysql_", properties))
	comm.offset = offset
	result_ranks =  comm.retrieve_all("RANK", "*")
	dico_ranks = {name : code for code, name in result_ranks}
	# Generate the taxons (obj) list
	taxons = []
	dico_names = {l.split('|')[0].strip() : l.split('|')[1].strip() for l in fnames if len(l) > 3 and 'scientific' in l.split('|')[3].strip() }
	for l in fnodes:
		if len(l) > 3:
			sline = l.split('|')
			current_taxon = Taxon(ncbi_code = sline[0].strip() if isinstance(sline[0], str) else sline[0])
			current_taxon.parent = sline[1].strip() if isinstance(sline[1], str) else sline[1]
			current_taxon.rank = dico_ranks[sline[2].strip()] if sline[2].strip() in dico_ranks.keys() else 'Null'
			# scientific name
			current_taxon.sname = dico_names[current_taxon.code]
			# add in current list of Taxons
			taxons.append(current_taxon)
	# Create a list of tuples with taxons objects
	values = [(t.code, t.parent, t.rank, t.sname) for t in taxons]
	# Write in database the taxons objects
	comm.insertmany("TAXON", values, TAXON_NCBI_CODE=1, ID_PARENT=2, ID_RANK=3, SCIENTIFIC_NAME=4)
	return
	
		
def launch(properties, arguments):
	# re-build database
	comm = connection_manager(**properties_part("mysql_", properties))
	mysql_patterns.reload_primary_pattern(comm)
	
	fnodes = read_a_file("nodes.dmp", properties)
	fnames = read_a_file("names.dmp", properties)

	manage_ranktable(fnodes,properties)
	manage_taxontable(fnodes, fnames, properties, int(arguments.offset))
	 	
def launch_continue(properties, arguments):
	fnodes = read_a_file("nodes.dmp", properties)
	fnames = read_a_file("names.dmp", properties)

	manage_ranktable(fnodes,properties)
	manage_taxontable(fnodes, fnames, properties, int(arguments.offset))

def properties_part(part, properties):
	return {k : v for k,v in properties.items() if k.startswith(part) and v != ''}

def init_arguments():
	arguments_parser = argparse.ArgumentParser()
	arguments_parser.add_argument('-a', "--all", action='store_true', dest='auto', default=False, help="automatic mode. do all actions.")
	arguments_parser.add_argument('--conf-file', action='store', dest='conf_file', default="./init.properties", help="configuration file, default = ./init.properties")
	arguments_parser.add_argument('-c', action='store_true', dest='continue_transfert', default=False, help="continue a previously mysql transfert (offset value needed!).")
	arguments_parser.add_argument('--offset', action='store', dest='offset', default=0, type=int, help="define the offset to continue a mysql transfert. Work only with '-c' option.")
	arguments_parser.add_argument('-d', '--download', action='store_true', dest='download', default=False, help="Download only")
	arguments_parser.add_argument('-D', '--exclude-download', action='store_true', dest='exclude_download', default=False, help="No download option.")
	arguments_parser.add_argument('-f', action='store_true', dest='force', default=False, help="Foce the download (work only with download mode)")
	return arguments_parser
	
if __name__ == "__main__":
	
	arguments_parser = init_arguments()
	arguments = arguments_parser.parse_args()
	properties = retrieve_properties(arguments.conf_file)
	
	configure_logger(properties["file_log"])
	
	if arguments.continue_transfert and arguments.offset != 0:
		logging.warning("continue transfert")
		launch_continue(properties, arguments)
		sys.exit(0)
		
	if arguments.exclude_download:
		logging.warning("launching without download")
		launch(properties, arguments)
		sys.exit(0)
		
	if arguments.download:
		logging.warning("download only")
		download_it(properties, arguments.force)
		sys.exit(0)
	
	if arguments.auto:
		download_it(properties, arguments.force)
		launch(properties, arguments)
		sys.exit(0)
		
	arguments_parser.print_help()

