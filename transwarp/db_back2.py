#!/usr/bin/python
import threading,functools

class _Engine(object):
	def __init__(self, connect):
		self._connect = connect
	def connect(self):
		return self._connect()

engine = None
	
class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None
	
	def init(self):
		self.connection = _LasyConnection()
		self.transactions = 0
	
	def cleanup(self):
		#self.connection.cleanup()
		self.connection = None

	def cursor(self):
		return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()
#def connection():
#	return _ConnectionCtx()
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
	if len(d)!=1:
		raise MultoColumnsError('Except only one column.')
	return d.values()[0]
@with_connection
def select(sql,*args):
	global engine
	print type(engine)
	return _select(sql,False,*args)
@with_connection
def insert(table, **kw):
	cols, args = zip(*kw.iteritems())
	sql = 'insert into %s (%s) values(%s)'%(table,','.join(['%s'%col for col in cols]),','.join(['?' for i in range(len(cols))]))
	print ('sql %s args %s'%(sql,str(args)))
	return _update(sql,*args)

	pass
@with_connection
def _update(sql, *args):
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
class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions==0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if  self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except:
			_db_ctx.connection.rollback()
			raisee
	
	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()
