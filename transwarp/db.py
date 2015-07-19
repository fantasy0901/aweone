#!usr/bin/python
# -*- coding: utf-8 -*-

import time, uuid, functools, threading, logging

class Dict(dict):
	'''
	Simple dict but support access as x,y style.
	'''
	def __init__(self,names=(), values=(),**kw):
		super(Dict,self).__init__(**kw)
		for k, v in zip(names, values):
			self[k]=v

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"`Dict` object has no attribute '%s'"%key)
	
	def __setattr__(self,key,value):
		self[key]=value
class DBError(Exception):
	pass
class MultiColumnsError(Exception):
	pass
engine = None
class _DbCtx(threading.local):
	
	def __init__(self):
		self.connection = None
		self.transactions = 0
	
	def is_init(self):
		return not self.connection is None
	
	def init(self):
		self.connection = engine
		self.transactions = 0
	def cleanup(self):
		self.connection = None 
	def cursor(self):	
		return self.connection.cursor()
def create_engine(user,password,database,host='127.0.0.1',port=3306,**kw):
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized.')
	params = dict(user=user,password=password,database=database,host=host,port=port)
	defaults = dict(use_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
	#print ('%s %s %s %s %s')%(user,password,database,host,port)
	for k,v in defaults.iteritems():
		params[k]= kw.pop(k,v)
	params.update(kw)
	params['buffered']=True
	engine = mysql.connector.connect(**params)
	print type(engine)
_db_ctx = _DbCtx()
class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			cursor = engine.cursor()
			_db_ctx.init()
			self.should_cleanup = True
		return self
	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def with_connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper

def _select(sql,first,*args):
	cursor = None
	sql = sql.replace('?','%s')
	global _db_ctx
	try:
		cursor = _db_ctx.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not values:
				return None
			return Dict(names,values)
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()
@with_connection
def select_one(sql,*args):
	return _select(sql,True,*args)
@with_connection
def select_int(sql,*args):
	d = _select(sql,True,*args)
	if len(d) !=1:
		raise MultoColumnsError('Except only one colum.')
	return d.values()[0]
@with_connection
def select(sql,*args):
	global engine
	print type(engine)
	return _select(sql,False,*args)
@with_connection
def _update(sql,*args):
	cursor = None
	global _db_ctx
	sql = sql.replace('?','%s')
	print sql
	try:
		cursor = _db_ctx.cursor()
		cursor.execute(sql,args)
		r = cursor.rowcount
		engine.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def insert(table,**kw):
	cols, args = zip(*kw.iteritems())
	sql = 'insert into %s (%s) values(%s)'%(table,','.join(['%s' % col for col in cols]),','.join(['?' for i in range(len(cols))]))
	print('sql %s args %s'%(sql,str(args)))
	return _update(sql,*args)

create_engine(user='root',password='1',database='python')
u1 = select_one('select * from user where id=?',1)
print 'u1'
print u1
u3 = insert(table='user',id='4',name='Jay',books='NULL')
print u3
print 'start select()...'
u2 = select('select * from user')
for item in u2:
	print ('%s %s' %(item.name,item.id))
print 'name:%s id:%s'%(u1.name,u1.id)		
