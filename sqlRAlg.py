class Operation:
    def __init__(self, operation, parameters=[], join_char=", "):
        self.operation = operation
        self.join_char = join_char
        if isinstance(parameters, str):
            self.parameters = parameters
        else:
            self.parameters = [ param for param in parameters ]
            print(type(self))
            print("Copied parameters %s" % self.parameters)
            print("Given parameters %s" % parameters)
        
    def __str__(self):
        ret_str = self.operation.upper()
        if type(self.parameters) is str:
            ret_str = "%s %s" % (ret_str, self.parameters)
        if type(self.parameters) is list and len(self.parameters) > 0:
            print("Parameters: %s" % self.parameters)
            ret_str = "%s %s" % (ret_str, self.join_char.join(self.parameters))
        return ret_str

        
class UnaryOperation(Operation):
    def __init__(self, operation, target, parameters=[], join_char=", "):
        super().__init__(operation, parameters, join_char)
        self.target = target
        
    def __str__(self):
        return "{0} ({1})".format(super().__str__(), self.target)
        
    
class BinaryOperation(Operation):
    def __init__(self, operation, lhs, rhs, parameters=[]):
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