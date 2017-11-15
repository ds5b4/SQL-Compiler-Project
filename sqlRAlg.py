""" Operation classes for easy string representation and tree construction of
relational algebra operations.

Authors:   John Maruska, David Strickland
Course:    CS 5300 - Database Systems
Professor: Dr. Alireza Hurson
Project:   SQL Compiler
Due Date:  2017-11-30 """


class Operation:
    """ Base class to be inherited from. Simply wrapper to generate string
    representations for relational algebra. """
    def __init__(self, operator, parameters=None, join_char=", "):
        if parameters is None:
            parameters = []

        self.operator = operator
        self.join_char = join_char
        self.children = []
        if isinstance(parameters, str):
            self.parameters = parameters
        else:
            self.parameters = [param for param in parameters]

    def __repr__(self):
        return self.base_repr()

    def base_repr(self):
        """ Workaround so derived classes get simple operation-only representation"""
        ret_str = self.operator.upper()
        if type(self.parameters) is str:
            ret_str = "%s %s" % (ret_str, self.parameters)
        if type(self.parameters) is list and len(self.parameters) > 0:
            ret_str = "%s %s" % (ret_str, self.join_char.join(self.parameters))
        return ret_str

        
class UnaryOperation(Operation):
    """ Represents any unary operation in relational algebra. Accepts a single
    target string, and optionally a list or string of parameters for the
    operation. """
    def __init__(self, operator, target, parameters=None, join_char=", "):
        if parameters is None:
            parameters = []

        super().__init__(operator, parameters, join_char)
        self.target = target
        
    def __repr__(self):
        return "{0} ({1})".format(super().__repr__(), self.target)


class BinaryOperation(Operation):
    """ Represents any binary operation in relational algebra. Accepts a
    left-hand side target, a right-hand side target, and an optional list or
    string of parameters for the operation. """
    def __init__(self, operator, lhs, rhs, parameters=None):
        if parameters is None:
            parameters = []

        super().__init__(operator, parameters)
        self.lhs = lhs
        self.rhs = rhs
        
    def __repr__(self):
        lhs = self.lhs
        rhs = self.rhs
        if isinstance(self.lhs, Operation):
            lhs = "(%s)" % self.lhs
        if isinstance(self.rhs, Operation):
            rhs = "(%s)" % self.rhs
        return "{0} {1} {2}".format(lhs, super().__repr__(), rhs)
