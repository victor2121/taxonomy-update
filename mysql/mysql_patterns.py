#!/bin/python
# -*-coding:utf-8 -*-

import sql_builder as sqlbuild
import mysql

def reload_primary_pattern(communicator):
	
	communicator.create_table("TAXON",\
	 "ID_TAXON INT PRIMARY KEY AUTO_INCREMENT, ",\
	 "TAXON_NCBI_CODE INT NOT NULL ,", "ID_PARENT INT, ", "ID_RANK INT, ", "SCIENTIFIC_NAME VARCHAR(150)")
	
	communicator.create_table("RANK",\
	 "ID_RANK INT PRIMARY KEY AUTO_INCREMENT, ",\
	  "NAME VARCHAR(100) NOT NULL")

	# Adding constraints
	
	communicator.alter_table("TAXON",\
	 "ADD CONSTRAINT fk_taxon_rank ",\
	 "FOREIGN KEY (ID_RANK) ",\
	 "REFERENCES RANK(ID_RANK) ")
	
	
if __name__=="__main__":
	reload_primary_pattern()

