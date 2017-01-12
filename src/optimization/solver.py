'''
Created on Jan 5, 2017

@author: henok.s.mengistu
'''
from pulp import *
from gurobipy import * 

class Solver():

    def __init__(self):
        pass

class CbcSolver(Solver):
    
    def __init__(self, name ):
        self.problem = None
        self.status = None
        self.name = name
    """
    return number of objectives
    """
    def get_objectives_count(self):
        pass  
    """
    return number of constraints
    """
    def get_contraint_count(self):
        pass
    """
    return number of variables
    """
    def get_variable_count(self):
        pass
    def create_problem(self,job_name,objective):
        if objective =='MIN':
            self.problem = LpProblem(job_name,LpMinimize)
        else:
            self.problem = LpProblem(job_name,LpMaximize)
        return self.problem
        
    def add_objective(self,objective,tag=None):
        self.problem += lpSum(objective),tag
        
    def add_constraint(self,constraint,operation,value,tag=None):
        if  operation == 'eq':
            self.problem += lpSum(constraint) == value,tag
        if  operation == 'lte':
            self.problem += lpSum(constraint) <= value,tag
        if  operation == 'gte':
            self.problem += lpSum(constraint) >= value,tag

    def add_constraintdivision(self,constraint,division,operation,value,tag):
        if  operation == 'eq':
            self.problem += lpSum(constraint)/division == value,tag
        if  operation == 'lte':
            self.problem += lpSum(constraint)/division <= value,tag
        if  operation == 'gte':
            self.problem += lpSum(constraint)/division >= value,tag
        
    def add_variables(self,name,stores,categories,space_levels,lower_bound):
        self.selected_tier = LpVariable.dicts(name, (stores, categories, space_levels), lower_bound, upBound=1,cat='Binary')

        return self.selected_tier

    def create_variables(self, name, categories, space_levels, lower_bound):
        self.created_tier = LpVariable.dicts(name, (categories, space_levels), lower_bound, upBound=1, cat='Binary')

        return self.created_tier

    def solveProblem(self):
        self.problem.solve(pulp.PULP_CBC_CMD(msg=2))
        self.status = LpStatus[self.problem.status]

    def getStatus(self):
        return self.status
    

class GurobiSolver(Solver):
    def __init__(self,name):
        self.gurobi_solver = Model(name)    
    """
    adds variables 
    """
    def add_variables(self,name,stores,categories,space_levels,lower_bound):
        pass