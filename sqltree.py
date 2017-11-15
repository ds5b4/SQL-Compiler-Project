""" Tree-related objects for easy modification of query trees.

Author:    John Maruska, David Strickland
Course:    CS 5300 - Database Systems
Professor: Dr. Alireza Hurson
Project:   SQL Compiler
Due Date:  2017-11-30 """

from sqlRAlg import Operation, UnaryOperation, BinaryOperation


class Node:
    """ Generic Node. Arbitrary location in query tree. """
    def __init__(self, parent=None, children=None):
        self.parent = parent

        if children is not None:
            self.children = [child for child in children]
        else:
            self.children = []

        if parent is not None:
            self.depth = parent.depth + 1
        else:
            self.depth = 0

    @property
    def root(self):
        """ Quick check if Node is the root node. """
        return self.parent is None


class OpNode(Node):
    """ Operator Node. Not restricted to any number of operands.
     :param operation: Operation object from sqlRAlg library. Current iteration
     expects either UnaryOperation or BinaryOperation
     :param parent: Parent Node. Default None, meaning root of its own tree. """
    def __init__(self, operation, parent=None):
        super().__init__(parent=parent)
        self.operation = operation

        if isinstance(BinaryOperation, operation):
            if type(operation.lhs) is Operation:
                lhs = OpNode(operation.lhs, parent=self)
            else:
                lhs = TableNode(operation.lhs, parent=self)

            if type(operation.rhs) is Operation:
                rhs = OpNode(operation.rhs, parent=self)
            else:
                rhs = TableNode(operation.rhs, parent=self)
            self.children = [lhs, rhs]

        elif isinstance(UnaryOperation, operation):
            if type(operation.lhs) is Operation:
                self.children = [OpNode(operation.target, parent=self)]
            else:
                self.children = [TableNode(operation.target, parent=self)]

        else:
            raise TypeError


class TableNode(Node):
    """ Table Node. Expecting table to be a simple string."""
    def __init__(self, table, parent=None):
        super().__init__(parent=parent)
        self.table = table
