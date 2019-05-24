#!/usr/bash/env python3
# -*- coding: utf-8 -*-

__author__ = 'bruce.liu'

"""
define  mysql  ORM
"""

import asyncio , logging

import aiomysql

def log(sql , args=()):
	logging.info('SQL : %S' % sql)

async def create_pool(loop , **kw):
	logging.info('create databse connection pool...')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		atuocommit=kw.get('atuocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
	)

async def select(sql , args ,size=None):
	log(sql , args)
	global __pool
	async with __pool.get() as conn:
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.replace('?' , '%s'), args or ())
			if size:
				rs = await cur.fetchmany(size)
			else:
				rs = await cur.fetchall()
		logging.info('rows returned %s' % len(rs))
		return rs

async def execute(sql , args . atuocommit=True):
	log(sql,args)
	async with __pool.get() as conn:
		if not atuocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?' , '%s') , args)
				affected = cur.rowcount()
			if not atuocommit:
				await conn.commit()
		except BaseException as e:
			if not atuocommit:
				conn.rollback()
			raise
		return affected

def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	return ' , '.join(L)

class Field(object):
	
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__ , self.column_type , self.name)	

	__repr__ = __str__

class StringField(Field):
	
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)	


class BooleanField(Field):
	
	def __init__(self, name=None, default=False):
		super().__init__(name, 'boolean', False, default)	


class IntegerField(Field):
	
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)	


class FloatField(Field):
	
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'double', primary_key, default)	


class TextField(Field):
	
	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)	


class ModelMetaclass(type):

	def __new__(cls, name, bases, attrs):
		if name == 'Nodel':
			return type.__new__(cls, name, bases, attrs)
		tableName = attrs.get('__table__' , None) or name 
		logging.info('Found Model: %s (table: %s)' % (name , tableName))
		mappings = dict()
		fields = []
		primarykey = None
		for k , v in attrs.items():
			if isinstance(v , Field):
				logging.info('  found mappings  %s ===>  %s ' % (k , v))
				mappings[k] = v 
				if v.primary_key:
					#找到主键
					if primarykey:
						raise Exception('Two primary key for field : %s' % k)
					primarykey = k 
				else:
					field.append(k)
		if not primarykey:
			raise Exception('Not Found primary key')
		#用pop把列的名称和field添加到attrs中，可以被items()出来
		for k in mappings.keys():
			attrs.pop(k)
		escaped_field = list(map(lambda f : '`%s`' % f , field)) 
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primarykey # 主键属性名
		attrs['__fields__'] = fields # 除主键外的属性名
		attrs['__select__'] = 'select `%s`, %s from  `%s`' % (primarykey , ',  '.join(escaped_field), tableName)
		attrs['__insert__'] = 'insert into `%s` (`%s` , %s) values (%s)' % (tableName , primarykey , ',  '.join(escaped_field), create_args_string(len(escaped_field) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)