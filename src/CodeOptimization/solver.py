'''
Created on Jan 5, 2017

@author: henok.s.mengistu
'''
from pulp import *

class Solver():

    def __init__(self):
        pass

class CbcSolver(Solver):
    
    def __init__(self):
        self.problem = None
    
    def createLpProblem(self,job_ame,objective):
        if objective =='MIN':
            self.problem = LpProblem(jobName,LpMinimize)
        else:
            self.problem = LpProblem(jobName,LpMaximize)                    
        
    def addObjective(self,objective,tag):
        self.problem += lpSum(objective,tag)
        
    def addConstraints(self,constraints,operation):
        pass
    def addVariable(self):
        pass
    
class GurobiSolver(Solver):
        