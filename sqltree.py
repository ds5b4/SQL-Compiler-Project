""" Tree-related objects for easy modification of query trees.

Author:    John Maruska, David Strickland
Course:    CS 5300 - Database Systems
Professor: Dr. Alireza Hurson
Project:   SQL Compiler
Due Date:  2017-11-30 """

from sqlRAlg import Operation, UnaryOperation, BinaryOperation


class Node:
    """ Generic Node. Arbitrary location in query tree. """
    def __init__(self, operation, parent=None):
        self.parent = parent
        self.children = []
        self.operation = operation

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
        super().__init__(operation, parent=parent)
        if isinstance(operation, BinaryOperation):
            if isinstance(operation.lhs, Operation):
                lhs = OpNode(operation.lhs, parent=self)
            else:
                lhs = TableNode(operation.lhs, parent=self)

            if isinstance(operation.rhs, Operation):
                rhs = OpNode(operation.rhs, parent=self)
            else:
                rhs = TableNode(operation.rhs, parent=self)

            self.children = [lhs, rhs]

        elif isinstance(operation, UnaryOperation):
            if isinstance(operation.target, Operation):
                self.children = [OpNode(operation.target, parent=self)]
            else:
                self.children = [TableNode(operation.target, parent=self)]

        else:
            raise TypeError

    def __str__(self):
        return self.operation.base_repr()


class TableNode(Node):
    """ Table Node. Expecting table to be a simple string."""
    def __init__(self, table, parent=None):
        super().__init__(table, parent=parent)

    def __str__(self):
        return self.operation


def print_tree(current_node, indent=""):
    """ Pretty print formatted tree object """
    print(indent + "-- %s " % current_node)
    for child in current_node.children:
        if isinstance(child, Node):
            print_tree(child, indent+"    ")
