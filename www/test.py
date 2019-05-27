#!/usr/bash/env python3
# -*- coding: utf-8 -*-

import orm,asyncio
from models import User, Blog, Comment

async def test(loop):
	await orm.create_pool(loop=loop, user='www-data', password='www-data', db='awesome')

	u = User(email='test@example.com', passwd='12345678', name='dedong', image='about:link')

	await u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()