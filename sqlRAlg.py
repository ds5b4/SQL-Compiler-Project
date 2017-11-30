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

SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}


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
        ret_str = self.operator
        if type(self.parameters) is str:
            ret_str += " " + str(self.parameters)
        if type(self.parameters) is list and len(self.parameters) > 0:
            def transform(x):
                """ Convert an arbitrary Attribute term to a print-str form """
                x_str = ""
                if type(x) == Condition:
                    for i in x:
                        if type(i) == Attribute:
                            x_str += "{0}.{1}".format(i.key, i.value)
                        else:
                            x_str += i
                elif type(x) == Attribute:
                    x_str += "{0}.{1}".format(x.key, x.value)
                elif type(x) == str:
                    x_str += x
                else:
                    raise NotImplementedError

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

    def find_aliases(self, alias=None):
        """ Returns the list of aliases that exist in this subtree. """
        if alias is None:
            renames = self.find_operators("RENAME")
        else:
            renames = [r for r in self.find_operators("RENAME")
                       if r.parameters == alias]
        # Assuming singular parameter of RENAME operation is the alias
        # Assuming that the rhs of RENAME operation is a literal table
        return list(map(lambda x: tuple([x.parameters, x.rhs.table]),
                        renames))

        
class UnaryOperation(Operation):
    """ Represents any unary operation in relational algebra. Accepts a single
    target string, and optionally a list or string of parameters for the
    operation. """
    def __init__(self, operator, rhs, parameters=None, parent=None):
        super().__init__(operator, parameters=parameters, parent=parent)
        self.rhs = rhs
        self.rhs.parent = self

    def __repr__(self):
        prev_repr = super().__repr__()
        return "{0} ({1})".format(prev_repr, self.rhs)

    def __eq__(self, other):
        return type(other) == UnaryOperation and \
               self.rhs == other.rhs and \
               super().__eq__(other)

    @property
    def children(self):
        """ Accessor for derived property """
        return [self.rhs]

    @property
    def tree_repr(self):
        """ Base representation for printing as a tree node. """
        return super().__repr__()

    def destroy(self):
        """ Remove this node from the tree. """
        if self.parent is None:
            self = self.rhs  # NOTE: If things break, check here.
        if self.parent.rhs == self:
            self.parent.rhs = self.rhs
        else:
            self.parent.lhs = self.rhs
        self.rhs.parent = self.parent


class BinaryOperation(Operation):
    """ Represents any binary operation in relational algebra. Accepts a
    left-hand side target, a right-hand side target, and an optional list or
    string of parameters for the operation. """
    def __init__(self, operator, lhs, rhs, parameters=None, parent=None):
        super().__init__(operator, parameters)
        self.lhs = lhs
        self.rhs = rhs
        self.lhs.parent = self
        self.rhs.parent = self
        self.parent = parent
        
    def __repr__(self):
        lhs = self.lhs
        rhs = self.rhs
        if isinstance(self.lhs, Operation):
            lhs = "(%s)" % self.lhs
        if isinstance(self.rhs, Operation):
            rhs = "(%s)" % self.rhs
        return "{0} {1} {2}".format(lhs, super().__repr__(), rhs)

    def __eq__(self, other):
        return isinstance(other, BinaryOperation) and \
               self.lhs == other.lhs and \
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


def early_restrict(tree):
    """ Move restricts as close to table as possible. """
    restricts = tree.find_operators("RESTRICT")
    for r in restricts:
        # Temporary value to avoid dirtying list data
        params = [p for p in r.parameters]
        for p in params:
            if p.op == " in ":
                continue
            # Checks against literal value
            # We assume all LHS will be attribute, drop from check
            elif type(p.rhs) != Attribute:  # RHS not an attribute
                for x in tree.find_operators("RENAME"):
                    # Skip count and non-matching tables.
                    if type(p.lhs) == str and p.lhs[:5] == "count" or \
                            x.parameters != p.lhs.key:
                        continue
                    # Insert early restriction on that table
                    add_restrict_above(x, p)
                    # Remove original restriction
                    r.parameters.remove(p)
                    if len(r.parameters) == 0:
                        r.destroy()  # r is a UnaryOperation

            elif type(p.rhs) == Attribute:
                # Remove old constraint
                target = find_join(r, p.lhs.key, p.rhs.key)
                if not target:
                    continue
                add_restrict_above(target, p)
                r.parameters.remove(p)
                if len(r.parameters) == 0:
                    r.destroy()


def convert_joins(tree):
    """ Converts product(X) operators with a leading RESTRICT to a join
    operator, with restrict parameters as its condition. """
    products = tree.find_operators("X")
    for p in products:
        if p.parent.operator != "RESTRICT":
            continue
        join_params = p.parent.parameters

        # Will always be on rhs because restrict is unary
        p.parent.rhs = BinaryOperation("JOIN", p.lhs, p.rhs,
                                       parameters=join_params,
                                       parent=p.parent)
        p.parent.destroy()


def early_project(tree, projections=None):
    """ Move projects as close to table as possible. """
    if projections is None:
        projections = set()
        projs = set()
    else:
        projs = set([p for p in projections])

    if isinstance(tree, TableNode):
        for p in projections:
            if tree.table == p.key:
                add_project_above(tree, p)
        return
    elif tree.operator == "X" or tree.operator == "JOIN":
        apply_projections(tree, projections)
        add_projections_to_set(tree.parameters, projs)

    elif tree.operator == "RESTRICT":
        apply_projections(tree, projections)
        add_projections_to_set(tree.parameters, projs)
    elif tree.operator == "PROJECT":
        add_projections_to_set(tree.parameters, projs)
    elif tree.operator == "RENAME":
        apply_projections(tree, [p for p in projections
                                 if p.key == tree.parameters])

    for child in tree.children:
        early_project(child, projs)


def print_tree_recursive(current_node, prefix="", last_child=True, first=True):
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
            print_tree_recursive(child, new_prefix, first=False)
        else:
            print_tree_recursive(child, new_prefix, last_child=False,
                                 first=False)


def print_tree(tree, title=None):
    """ Non-recursive wrapper for printing tree form. Allows beginning and
    ending markings that don't interfere with structure. """
    if title is not None:
        print(title)
    print_tree_recursive(tree)
    print("")


# ==============================================================================


def find_join(node, name1, name2):
    """ Find and return the cartesian product Node which joins relations
    name1 and name2. """
    n1_aliased = name1 not in SCHEMA.keys()
    n2_aliased = name2 not in SCHEMA.keys()
    x = find_join_recurse(node, name1, name2, n1_aliased, n2_aliased)
    if type(x) == BinaryOperation:
        return x
    else:
        return False


def find_join_recurse(node, name1, name2, n1_aliased, n2_aliased):
    """ Find and return the cartesian product Node which joins relations
    name1 and name2. """
    found_1, found_2 = (False, False)
    for child in node.children:
        # Begin with children.
        x = find_join_recurse(child, name1, name2, n1_aliased, n2_aliased)
        if type(x) == BinaryOperation:
            return x
        f_1, f_2 = x
        found_1 = found_1 or f_1
        found_2 = found_2 or f_2

    if found_1 and found_2:
        if node.operator == "X":
            return node

    if n1_aliased:
        if isinstance(node, UnaryOperation) and node.operator == "RENAME":
            found_1 = found_1 or (node.parameters == name1)
    else:
        if isinstance(node, TableNode) and node.table == name1:
            found_1 = True

    if n2_aliased:
        if isinstance(node, UnaryOperation) and node.operator == "RENAME":
            found_2 = found_2 or (node.parameters == name2)
    else:
        if isinstance(node, TableNode) and node.table == name2:
            found_2 = True

    return found_1, found_2


def add_project_above(node, attr):
    """ Project an attribute above a given node. """
    parent = node.parent
    # NOTE: root node is always a project.
    if parent is None:
        return

    if not attr:
        return

    if parent.operator == "PROJECT":
        if attr not in parent.parameters:
            parent.parameters.append(attr)
    else:
        if parent.rhs == node:
            parent.rhs = UnaryOperation("PROJECT", node, [attr])
            parent.rhs.parent = parent
        elif parent.lhs == node:
            parent.lhs = UnaryOperation("PROJECT", node, [attr])
            parent.lhs.parent = parent
        else:
            raise AttributeError  # node not added as child to its parent


def add_projections_to_set(projections, proj_set):
    """ Take attributes from parameter list and add them to given set. """
    for p in projections:
        if type(p) == Attribute:
            proj_set.add(p)
        if type(p) == Condition:
            if type(p.lhs) == Attribute:
                proj_set.add(p.lhs)
            if type(p.rhs) == Attribute:
                proj_set.add(p.rhs)


def apply_projections(node, projs):
    """ Applies all relevant projections above node"""
    for p in projs:
        # find_tables() is truthy if table exists as descendant
        if node.find_aliases(p.key) or node.find_tables(p.key) \
                or (node.operator == "RENAME" and node.parameters == p.key) \
                or (type(node) == TableNode and node.table == p.key):
            add_project_above(node, p)


def add_restrict_above(node, condition):
    """ Add a restriction of `condition' above the given `node'. """
    parent = node.parent
    if parent.operator != "RESTRICT":
        if parent.rhs == node:
            parent.rhs = UnaryOperation("RESTRICT", node, [condition])
            parent.rhs.parent = parent
        else:
            parent.lhs = UnaryOperation("RESTRICT", node, [condition])
            parent.lhs.parent = parent
    else:
        parent.parameters.append(condition)
