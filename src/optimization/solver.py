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
    
    def createProblem(self,job_name,objective):
        if objective =='MIN':
            self.problem = LpProblem(job_name,LpMinimize)
        else:
            self.problem = LpProblem(job_name,LpMaximize)
        return self.problem
        
    def addObjective(self,objective,tag):
        self.problem += lpSum(objective),tag
        
    def addConstraint(self,constraint,operation,value,tag):
        if  operation == 'eq':
            self.problem += lpSum(constraint) == value,tag
        if  operation == 'lte':
            self.problem += lpSum(constraint) <= value,tag
        if  operation == 'gte':
            self.problem += lpSum(constraint) >= value,tag

    def addConstraintDivision(self,constraint,division,operation,value,tag):
        if  operation == 'eq':
            self.problem += lpSum(constraint)/division == value,tag
        if  operation == 'lte':
            self.problem += lpSum(constraint)/division <= value,tag
        if  operation == 'gte':
            self.problem += lpSum(constraint)/division >= value,tag
        
    def addVariables(self,name,stores,categories,space_levels,lower_bound):
        self.selected_tier = LpVariable.dicts(name, (stores, categories, space_levels), lower_bound, upBound=1,cat='Binary')

        return self.selected_tier

    def addCreateVariables(self, name, categories, space_levels, lower_bound):
        self.created_tier = LpVariable.dicts(name, (categories, space_levels), lower_bound, upBound=1, cat='Binary')

        return self.created_tier

    def solveProblem(self):
        return self.problem.solve(pulp.PULP_CBC_CMD(msg=2))