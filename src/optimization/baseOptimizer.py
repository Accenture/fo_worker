#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import numpy as np
import logging

class BaseOptimizer(object):
    """
    Created on Wed Jan 4 15:33:51 2017

    @author: omkar.marathe

    This is the base class for Different
    Optimization Algorithms
    """

    #def __init__(self,job_name,job_type,stores,categories,increment,sales_penetration_threshold):
    def __init__(self, sales,space,future_space,brand_exit,config):
        self.job_name = config['meta']['name']
        self.job_type = config['jobType']

        self.categories = config['salesCategories']
        self.stores = space['Store'].unique()

        self.increment = config['increment']
        self.sales_penetration_threshold = config['salesPenetrationThreshold']
        self.lp_problem_status = None
        self.problem =  None

        self.sales = sales
        self.space = space
        self.future_space = future_space
        self.brand_exit = brand_exit
        self.config = config

    def get_lp_problem_status(self):
        return self.lp_problem_status

    def get_diagnostics(self):
        #Todo
        pass
    def get_solver(self):
        #Todo
        #pass
        return self.solver.name
    def set_solved(self):
        #Todo
        pass
    def get_mps(self):
        #Todo
        pass
    def get_lp(self):
        #Todo
        #pass
        return self.solver.problem
    def solvelp(self):
        #Todo
        pass
    def get_model_details(self):
        #Todo
        pass
    def round_value(self,cVal, increment):
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal

    def search_param(self,search, jobName):
        if search in jobName:
            begin = jobName.find(search)
            length = 0
            for char in jobName[(len(search) + begin)::]:
                try:
                    int(char)
                    length = length + 1
                except:
                    break
            try:
                search_params = int(jobName[(len(search) + begin):(len(search) + begin + length)]) / 100
                logging.info('{} has been changed to {}'.format(search,search_params))
                return searchParam
            except:
                return True
        else:
            return None        

    def optimize(self):
        raise NotImplementedError("Subclass must implement this abstract method")