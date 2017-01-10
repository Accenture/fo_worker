import unittest
import sys
import os
sys.path.append(os.path.abspath('../src/'))
from detachedWorker import *
from helper import *
from optimization.dataMerger import DataMerger

class TestStringMethods(unittest.TestCase):

	def test_balance_back(self):
		_dir = 'balance_back_test/'
		conf = _dir + 'input/config.json'
		sales_data = _dir + 'input/Sales.csv'
		space_data = _dir + 'input/Space.csv'

		job = loadJson(conf)

		data_merger = DataMerger()

		if job['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(
				data_merger.read_sales_data(sales_data),data_merger.read_space_data(space_data),None,None,loadJson(conf))
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')


	"""
	Test suite for unit testing all optimization scenarios.
	created one test case, for children data set.
	each test case takes input sales,space,brand_exit and future space date. In prod env, user will update these files.

	Additional config.json is required, which has other metrics like job type, no of min/max tiers etc.
	In prod env, UI will create this object and creates this message.json and add to the RabbitMQ.
	Then fo_worker daemon process gets the message from queue and process the job

	"""
	if __name__ == '__main__':
		unittest.main()