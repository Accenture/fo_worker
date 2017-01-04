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

    def __init__(self,jobName,Stores,Categories,increment,salesPen,tierCounts=None):
        self.jobName = jobName            
        self.Stores = Stores
        self.Categories = Categories      
        self.increment = increment        
        self.salesPen = salesPen
        self.tierCounts = tierCounts

    def roundValue(cVal, increment):
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal

    def searchParam(search, jobName):
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
                searchParam = int(jobName[(len(search) + begin):(len(search) + begin + length)]) / 100
                logging.info('{} has been changed to {}'.format(search,searchParam))
                return searchParam
            except:
                return True
        else:
            return None        

