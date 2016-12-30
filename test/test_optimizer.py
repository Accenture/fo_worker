#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath('../src/'))
from detachedWorker import *
from helper import *

def test(test_dir):
	_dir = test_dir
	conf = _dir + 'input/config.json'
	sales_data = _dir + 'input/Sales.csv'
	space_data = _dir + 'input/Space.csv'
	future_space = None
	#future_space = _dir + 'input/Future_Space_data.csv'

	brand_exit = None
	drill_down = False

	lst = (_dir, conf, sales_data, space_data, space_data, future_space, brand_exit, drill_down)
	msg = modifyJson(lst)

	dataMerged, preOpt, problem, longOutput, wideOutput = run(msg)

	toCsv(dataMerged, _dir + 'intermediate/dataMerged.csv')
	toCsv(preOpt, _dir + 'intermediate/preOpt.csv')
	#problem.writeLP(_dir + 'lp_problem/tiered_traditional.lp')
	toCsv(longOutput, _dir + 'output/longOutput.csv')
	toCsv(wideOutput, _dir + 'output/wideOutput.csv')

"""
Test suite for unit testing all optimization scenarios.
created one test case, for children data set.
each test case takes input sales,space,brand_exit and future space date. In prod env, user will update these files.

Additional config.json is required, which has other metrics like job type, no of min/max tiers etc.
In prod env, UI will create this object and creates this message.json and add to the RabbitMQ.
Then fo_worker daemon process gets the message from queue and process the job

"""
if __name__ == '__main__':
	test('test_children_tiered_traditional/')
	#test('test_small_children/test_tiered_traditional/')


