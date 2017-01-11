import unittest
import sys
import os
sys.path.append(os.path.abspath('../src/'))
import os
import os.path
from detachedWorker import *
from helper import *
from optimization.dataMerger import DataMerger

class TestOptimizers(unittest.TestCase):

	def get_input_data(self,_dir):
		conf = _dir + 'config.json'
		sales_data = _dir + 'Sales.csv'
		space_data = _dir + 'Space.csv'
		future_space_data = _dir + 'Future_Space.csv'
		brand_exit_data = _dir + 'Brand_Exit.csv'

		data_merger = DataMerger()

		if os.path.exists(future_space_data):
			future_space = data_merger.read_future_space_data(future_space_data)
		else:
			future_space = None

		if os.path.exists(brand_exit_data):
			brand_exit = data_merger.read_brand_exit_data(brand_exit_data)
		else:
			brand_exit = None

		(sales,space,future_space,exit,config) = \
				( data_merger.read_sales_data(sales_data),
				  data_merger.read_space_data(space_data),
				  future_space,
				  brand_exit,
				  loadJson(conf)
			  	)
		return (sales,space,future_space,exit,config)


	def ptest_balance_back(self):
		_dir = 'balance_back_test/input/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')

	def ftest_Children_Scn_Trdtnl_TC01(self):
		_dir = 'Test_Data_Trad/Children_Scn_Trdtnl_TC01/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')

	def ftest_Intimates_Scn_Trdtnl_TC02(self):
		_dir = 'Test_Data_Trad/Intimates_Scn_Trdtnl_TC02/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')


	def ftest_Misses_Scn_Trdtnl_TC03(self):
		_dir = 'Test_Data_Trad/Misses_Scn_Trdtnl_TC03/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')

	def test_Children_Scn_Trdtnl_UNCON_TC11(self):
		_dir = 'Test_Data_Trad/Children_Scn_Trdtnl_UNCON_TC11/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')

	def ftest_Intimates_Scn_Trdtnl_UNCON_TC12(self):
		_dir = 'Test_Data_Trad/Intimates_Scn_Trdtnl_UNCON_TC12/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
			optimRes = optimizer.optimize()
			self.assertEqual(optimizer.get_lp_problem_status(), 'Optimal')

	def ftest_Misses_Scn_Trdtnl_UNCON_TC13(self):
		_dir = 'Test_Data_Trad/Misses_Scn_Trdtnl_UNCON_TC13/'
		(sales, space, future_space, exit, config) = self.get_input_data(_dir)

		if config['optimizationType'] == 'traditional':
			optimizer = TraditionalOptimizer(sales, space, future_space, exit, config)
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