'''
Created on Jan 5, 2017

@author: henok.s.mengistu
@author: omkar.marathe
'''
from pulp import *
from gurobipy import * 
from pip.cmdoptions import constraints

class Solver():
    def __init__(self):
        pass

class CbcSolver(Solver):
    
    def __init__(self, name ):
        self.problem = None
        self.status = None
        self.name = name
        self.objectives_count = 0   

    """
    return number of objectives
    """
    def get_objectives_count(self):
        return self.objectives_count
  
    """
    return number of constraints
    """
    def get_constraint_count(self):
        return len(self.problem.constraints)

    """
    return number of variables
    """
    def get_variable_count(self):
        return len(self.problem.variables())    

    """
    return Objectives 
    """
    def get_objectives(self):
        return self.problem.objective        
    """
    return constraints
    """
    def get_constraints(self):
        return self.problem.constraints
    """
    return Lp Variables 
    """
    def get_variables(self):
        return self.problem.variables()
    
    def create_problem(self,objective,job_name):
        if objective =='MIN':
            self.problem = LpProblem(job_name,LpMinimize)
        else:
            self.problem = LpProblem(job_name,LpMaximize)
        
        #what is the need for this return statement 
        return self.problem
        
    def add_objective(self,selected_tier,error,stores,categories,space_levels,tag):        
        self.problem += lpSum([(selected_tier[store][category][level] * error[i][j][k]) \
                         for (i, store) in enumerate(stores) \
                            for (j, category) in enumerate(categories) \
                                for (k, level) in enumerate(space_levels)]), tag
        self.objectives_count += 1
                                    
        #self.problem += lpSum(objective),tag
        
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
        self.gurobi_model = Model(name)      
        self.status_codes = {1:"LOADED",
                             2:"OPTIMAL",
                             3:"INFEASIBLE",
                             4:"INF_OR_UNBD",
                             5:"UNBOUNDED",
                             6:"CUTOFF",
                             7:"ITERATION_LIMIT",
                             8:"NODE_LIMIT",
                             9:"TIME_LIMIT",
                             10:"SOLUTION_LIMIT",
                             11:"INTERRUPTED",
                             12:"NUMERIC",
                             13:"SUBOPTIMAL",
                             14:"INPROGRESS",
                             15:"USER_OBJ_LIMIT"}  
    
    """
    add variables 
    """
    def format_name(self,name_string):
        return name_string.replace(" ","_")
        
    def add_variables(self,names,stores,categories,space_levels,lower_bound):                
        store_category_level = {}
        for (i, store) in enumerate(stores):
            store_category_level[store] = {}
            for (j, category) in enumerate(categories):
                store_category_level[store][category] = {}
                for (k, level) in enumerate(space_levels):
                    store_category_level[store][category][level]=self.gurobi_model.addVar(obj=0,lb=lower_bound,ub=1,vtype="B",\
                                                                                         name=self.format_name(names)+\
                                                                                         "_%s_%s_%s"%(store,category,level))
        self.gurobi_model.update()
        return store_category_level   
                
    def create_variables(self, names, categories, space_levels, lower_bound):
        category_level = {}
        for (j, category) in enumerate(categories):
                category_level[category] = {}
                for (k, level) in enumerate(space_levels):
                    category_level[category][level]=self.gurobi_model.addVar(obj=0,lb=lower_bound,ub=1,vtype="B",\
                                                                                         name=self.format_name(names)+\
                                                                                         "_%s_%s"%(category,level))
        self.gurobi_model.update()        
        return category_level
    
    def add_objective(self,selected_tier,error,stores,categories,space_levels,tag=None):
        objectives = None
        for (i, store) in enumerate(stores):
            for (j, category) in enumerate(categories):
                for (k, level) in enumerate(space_levels):
                    objectives+=selected_tier[store][category][level] * error[i][j][k]                           
        
        self.gurobi_model.setObjective(objectives,None)  
        self.gurobi_model.update()      
    
    def create_problem(self,job_name,objective):
        if objective =='MIN':
            self.gurobi_model.ModelSense = GRB.MINIMIZE
        else:
            self.gurobi_model.ModelSense = GRB.MAXIMIZE
     
    def add_constraint(self,constraint,operation,value,tag=None):
        for const in constraint:                               
            if  operation == 'eq':           
                self.gurobi_model.addConstr(const,GRB.EQUAL, value,tag)
            if  operation == 'lte':
                self.gurobi_model.addConstr(const, GRB.LESS_EQUAL, value,tag)            
            if  operation == 'gte':
                self.gurobi_model.addConstr(const, GRB.GREATER_EQUAL, value,tag)                    
        
        # for debugging purposes         
        #         for v in self.gurobi_model.getConstrs():
        #             print (v)                    
        
    def add_constraintdivision(self,constraint,division,operation,value,tag):
        #print (constraint)
        for const in constraint:
            if  operation == 'eq':           
                self.gurobi_model.addConstr(const/division,GRB.EQUAL, value,tag)
            if  operation == 'lte':
                self.gurobi_model.addConstr(const/division, GRB.LESS_EQUAL,value,tag)            
            if  operation == 'gte':
                self.gurobi_model.addConstr(const/division, GRB.GREATER_EQUAL,value,tag)  
    
    def solveProblem(self):
        self.gurobi_model.optimize()          
        self.status = self.status_codes[self.gurobi_model.getAttr(GRB.Attr.Status)]   
        self.gurobi_model.write("gurobiLp.lp")    