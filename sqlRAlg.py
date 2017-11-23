""" Operation classes for easy string representation and tree construction of
relational algebra operations.

Authors:   John Maruska, David Strickland
Course:    CS 5300 - Database Systems
Professor: Dr. Alireza Hurson
Project:   SQL Compiler
Due Date:  2017-11-30 """

from collections import namedtuple

Attribute = namedtuple('Attribute', ['key', 'value'])
Condition = namedtuple('Condition', ['lhs', 'op', 'rhs'])


class Operation:
    """ Base class to be inherited from. Simply wrapper to generate string
    representations for relational algebra. """
    def __init__(self, operator, parameters=None, parent=None):
        # Given parameters
        self.operator = operator
        if parameters is None:
            parameters = []

        if isinstance(parameters, str):
            self.parameters = parameters
        else:
            self.parameters = [param for param in parameters]

        self.join_char = " and " if operator == "RESTRICT" else ", "

        self.parent = parent

    def __repr__(self):
        """ Workaround so derived classes get simple representation """
        ret_str = self.operator.upper()
        if type(self.parameters) is str:
            ret_str += " " + str(self.parameters)
        if type(self.parameters) is list and len(self.parameters) > 0:
            def transform(x):
                """ Convert an arbitrary Condition term to a print-str form """
                x_str = ""
                for i in x:
                    if type(i) == Attribute:
                        x_str += "{0}.{1}".format(i.key, i.value)
                    else:
                        x_str += i
                return x_str
            ret_str += " " + self.join_char.join(list(map(transform,
                                                          self.parameters)))

        return ret_str

    def __eq__(self, other):
        return self.operator == other.operator and \
               self.parameters == other.parameters and \
               self.depth == other.depth

    @property
    def depth(self):
        """ Accessor for derived property. """
        return 0 if self.root else self.parent.depth + 1

    @property
    def root(self):
        """ Quick check if Node is the root node. """
        return self.parent is None

    @property
    def children(self):
        """ Break if not implemented on derived classes. Do not call on main
        instance. """
        raise NotImplementedError

    def find_operators(self, op):
        """ Depth first search that generates a list of Operation nodes that
        are descendants of the Operation. """
        operators = []
        for child in self.children:
            if isinstance(child, Operation):
                if child.operator == op:
                    operators.append(child)
                operators += child.find_operators(op)
        return operators

    def find_tables(self, name=None):
        """ Depth first search that generates a list of TableNodes that are
        descendants of the Operation. """
        tables = []
        for child in self.children:
            if isinstance(child, Operation):
                tables += child.find_tables()
            elif isinstance(child, TableNode):
                if name is not None and child.table == name:
                    tables.append(child)
        return tables

    def find_aliases(self):
        """ Returns the list of aliases that exist in this subtree. """
        renames = self.find_operators("RENAME")
        # Assuming singular parameter of RENAME operation is the alias
        # Assuming that the rhs of RENAME operation is a literal table
        return list(map(lambda x: tuple([x.parameters[0], x.rhs.table]),
                        renames))

    def early_restrict(self):
        """ Move restricts as close to table as possible. """
        # TODO: Handle case of early restricting to join
        # TODO: samples/03.txt did not go through
        # TODO: samples/04.txt did not move r.rating>5
        # TODO: samples/05.txt did not move
        # TODO: samples/11.txt has an extra condition appended on RESTRICT.
        restricts = self.find_operators("RESTRICT")
        for r in restricts:
            for p in r.parameters:
                # Yo how does "IN" even work
                if p.op == " in ":
                    continue
                # TODO: If we assume all LHS will be attribute, drop from check
                # Checks against literal value
                # TODO: We have no way to tell if aliased or not.
                elif type(p.lhs) == Attribute and type(p.rhs) != Attribute:
                    print(p)
                    for x in self.find_operators("RENAME"):
                        if x.parameters != p.lhs.key:
                            continue
                        aliased = True
                        # Insert early restriction
                        parent = x.parent
                        if parent.operator != "RESTRICT":
                            parent.rhs = UnaryOperation("RESTRICT", x, [p])
                        else:
                            parent.parameters.append(p)
                        # Remove original restriction
                        r.parameters.remove(p)
                        if len(r.parameters) == 0:
                            r.remove()  # r is a UnaryOperation
                elif type(p.lhs) == Attribute and type(p.rhs) == Attribute:
                    # print(p)
                    pass

        
class UnaryOperation(Operation):
    """ Represents any unary operation in relational algebra. Accepts a single
    target string, and optionally a list or string of parameters for the
    operation. """
    def __init__(self, operator, rhs, parameters=None,
                 parent=None):
        super().__init__(operator, parameters=parameters, parent=parent)
        self.rhs = rhs
        self.rhs.parent = self

    def __repr__(self):
        return "{0} ({1})".format(super().__repr__(), self.rhs)

    def __eq__(self, other):
        return self.rhs == other.rhs and \
               super().__eq__(other)

    @property
    def children(self):
        """ Accessor for derived property """
        return [self.rhs]

    @property
    def tree_repr(self):
        """ Base representation for printing as a tree node. """
        return super().__repr__()

    def remove(self):
        """ Remove this node from the tree. """
        if self.parent.rhs == self:
            self.parent.rhs = self.rhs
        else:
            self.parent.lhs = self.rhs
        self.rhs.parent = self.parent


class BinaryOperation(Operation):
    """ Represents any binary operation in relational algebra. Accepts a
    left-hand side target, a right-hand side target, and an optional list or
    string of parameters for the operation. """
    def __init__(self, operator, lhs, rhs, parameters=None):
        super().__init__(operator, parameters)
        self.lhs = lhs
        self.rhs = rhs

        self.lhs.parent = self
        self.rhs.parent = self
        
    def __repr__(self):
        lhs = self.lhs
        rhs = self.rhs
        if isinstance(self.lhs, Operation):
            lhs = "(%s)" % self.lhs
        if isinstance(self.rhs, Operation):
            rhs = "(%s)" % self.rhs
        return "{0} {1} {2}".format(lhs, super().__repr__(), rhs)

    def __eq__(self, other):
        return self.lhs == other.lhs and \
               self.rhs == other.rhs and \
               super().__eq__(other)

    @property
    def children(self):
        """ Accessor for derived property """
        return [self.lhs, self.rhs]

    @property
    def tree_repr(self):
        """ Base representation for printing as a tree node. """
        return super().__repr__()


class TableNode:
    """ Table Node. Expecting table to be a simple string."""
    def __init__(self, table, parent=None):
        self.parent = parent
        self.children = []
        self.table = table

    def __str__(self):
        return self.table

    def __eq__(self, other):
        return self.children == other.children and \
               self.table == other.table and \
               self.depth == other.depth

    @property
    def depth(self):
        """ Accessor for derived property. """
        return 0 if self.parent is None else self.parent.depth + 1

    @property
    def tree_repr(self):
        """ Base representation for printing as a tree node. """
        return self.table


def print_tree(current_node, prefix="", last_child=True, first=True):
    """ Pretty print formatted tree object """
    if first:
        print(">> %s" % current_node.tree_repr)
    else:
        print(prefix + '|___' + " %s" % current_node.tree_repr)

    for idx, child in enumerate(current_node.children):
        if not last_child:
            new_prefix = prefix+"|  "
        else:
            new_prefix = prefix+"   "

        if idx == len(current_node.children) - 1:
            print_tree(child, new_prefix, first=False)
        else:
            print_tree(child, new_prefix, last_child=False, first=False)


