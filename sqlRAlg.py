class Operation:
    """
    Base class to be inherited from. Simply wrapper to generate string
    representations for relational algebra.
    """
    def __init__(self, operation, parameters=None, join_char=", "):
        if parameters is None:
            parameters = []

        self.operation = operation
        self.join_char = join_char
        if isinstance(parameters, str):
            self.parameters = parameters
        else:
            self.parameters = [param for param in parameters]
        
    def __str__(self):
        ret_str = self.operation.upper()
        if type(self.parameters) is str:
            ret_str = "%s %s" % (ret_str, self.parameters)
        if type(self.parameters) is list and len(self.parameters) > 0:
            ret_str = "%s %s" % (ret_str, self.join_char.join(self.parameters))
        return ret_str

        
class UnaryOperation(Operation):
    """
    Represents any unary operation in relational algebra. Accepts a single
    target string, and optionally a list or string of parameters for the
    operation.
    """
    def __init__(self, operation, target, parameters=None, join_char=", "):
        if parameters is None:
            parameters = []

        super().__init__(operation, parameters, join_char)
        self.target = target
        
    def __str__(self):
        return "{0} ({1})".format(super().__str__(), self.target)
        
    
class BinaryOperation(Operation):
    """
    Represents any binary operation in relational algebra. Accepts a
    left-hand side target, a right-hand side target, and an optional list or
    string of parameters for the operation.
    """
    def __init__(self, operation, lhs, rhs, parameters=None):
        if parameters is None:
            parameters = []

        super().__init__(operation, parameters)
        self.lhs = lhs
        self.rhs = rhs
        
    def __str__(self):
        lhs = self.lhs
        rhs = self.rhs
        if isinstance(self.lhs, Operation):
            lhs = "(%s)" % self.lhs
        if isinstance(self.rhs, Operation):
            rhs = "(%s)" % self.rhs
        return "{0} {1} {2}".format(lhs, super().__str__(), rhs)
