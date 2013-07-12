#!/bin/python
# -*-coding:utf-8 -*-



def build_sql_insert(name, kwargs):
	query = "INSERT INTO " + name + "("
	keys = ''
	limit = len(kwargs)
	i = 0
	for kwarg in kwargs:
		i += 1
		keys += kwarg[0]
		if i < limit:
			keys += " ,"
	query += keys + ") VALUES("
	
	values = ''
	
	i = 0
	for key in kwargs:
		i += 1
		values += key[1]
		if i < limit:
			values += " ,"

	query += values + ")" 
	return query
	
def build_sql_createtable(name, args):
	query = "CREATE TABLE " + name + "("
	for arg in args:
		query += arg
	
	query += ")"
	return query

def build_sql_altertable(name, args):
	query = "ALTER TABLE " + name + ' '
	for arg in args:
		query += arg

	return query

def build_sql_select(name, args):
	query = "SELECT "
	i = 0
	limit = len(args)
	for arg in args:
		i += 1
		query += arg
		if i < limit:
			query += ", "
		
	query += " FROM " + name
	
	return query

