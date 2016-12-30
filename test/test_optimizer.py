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

if __name__ == '__main__':
	test('test_children_tiered_traditional/')
	#test('test_small_children/test_tiered_traditional/')


