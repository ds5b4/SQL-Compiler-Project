"""" Script takes in a SQL query through standard input, generates its relational
algebra, and prints out the result. Created for Missouri S&T CS5300 - Database
Systems semester project.

Author:    John Maruska, David Strickland
Course:    CS 5300 - Database Systems
Professor: Dr. Alireza Hurson
Project:   SQL Compiler
Due Date:  2017-11-30 """


import sys
from sqlRAlg import BinaryOperation, UnaryOperation
from sqltree import OpNode, print_tree

# TODO: Need to add GROUP BY


AGGREGATE_FUNCTIONS = ["ave", "max", "count"]
COMPARATOR_OPERATIONS = ['>=', '<=', '!=', '=', '>', '<', 'in']
JOIN_OPERATIONS = ["where", "group", "order", "having", "contains", "union",
                   "intersect", "except"]
SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}
COLUMNS = [column for _, table_cols in SCHEMA.items() for column in table_cols]
# noinspection PyRedeclaration
TABLES = [table for table, _ in SCHEMA.items()]

condition = {"lhs": '', "rhs": '', "op": ''}
iter_stopped = False
token = ""


class Query:
    """ Wrapping class that stores all query data such as tables, conditions,
    etc., as well as handling logic around the query such as relational algebra
    generation. Created so that queries can be easily nested. """
    def __init__(self, parent=None, child=None, join_operator=None):
        self.parent = parent
        if parent is not None:
            parent.child = self
        self.child = child
        if child is not None:
            child.parent = self
        self.join_operator = join_operator

        self.tables_included = set()
        self.tables_needed = set()
        self.project_needed = set()
        self.selects_needed = set()
        self.aggregates_needed = set()

        self.table_aliases_needed = dict()
        self.table_aliases_appeared = dict()
        self.query_table = dict()

        self.rel_alg = None
        self.condition_str = ""

    def __str__(self):
        return self.generate_relational_algebra()

    @property
    def relational_algebra(self):
        """ String of relational algebra representing query """
        if self.rel_alg is None:
            self.rel_alg = self.generate_relational_algebra()

        return self.rel_alg

    def generate_relational_algebra(self):
        """ Generates the relational algebra for the parsed query. """
        # Check all table aliases appear
        for alias in self.table_aliases_needed:
            if alias not in self.table_aliases_appeared:
                print("create_rel_alg: required alias %s did not appear" % alias)
                return False

        # Expand wildcard to names of columns
        if "*" in self.project_needed:
            # Assuming wildcard only appears first, not after other columns
            projections = [column for table in self.tables_included
                           for column in SCHEMA[table]]
            project_op = UnaryOperation("PROJECT", None, projections)
        # Use needed projections
        else:
            project_op = UnaryOperation("PROJECT", None, self.project_needed)

        # RESTRICT if there is a condition string, otherwise no restriction
        if len(self.condition_str) > 0:
            restrict_op = UnaryOperation("RESTRICT", None, self.condition_str)
        else:
            restrict_op = None

        # TODO: Might want to mix aliased and non-aliased.
        # If any tables aliased
        if len(self.table_aliases_appeared) > 0:  # if aliased tables
            def rename_table(table, al):
                """ Converts a table and its alias to appropriate RENAME / RHO
                relational algebra. """
                return UnaryOperation("RENAME", table, al)

            self.query_table = dict(self.table_aliases_appeared)

            # Only one aliased table
            if len(self.table_aliases_appeared) == 1:
                alias, table = self.table_aliases_appeared.popitem()
                child_op = rename_table(table, alias)
            # Several aliased tables
            else:  # len(table_aliases_appeared) >= 2:
                alias, table = self.table_aliases_appeared.popitem()
                r1 = rename_table(table, alias)

                alias, table = self.table_aliases_appeared.popitem()
                r2 = rename_table(table, alias)

                child_op = BinaryOperation("X", r1, r2)

            # Chain join renamed tables
            for alias, table in self.table_aliases_appeared.items():
                child_op = BinaryOperation("X", child_op,
                                                  rename_table(table, alias))

        # No tables aliased
        else:
            self.query_table = set(self.tables_included)
            # Must have at least one table
            if len(self.tables_included) < 1:
                raise ValueError
            # No joins needed
            elif len(self.tables_included) == 1:
                child_op = self.tables_included.pop()
            # Join tables together
            else:  # if len(tables_included) >= 2:
                t1 = self.tables_included.pop()
                t2 = self.tables_included.pop()
                child_op = BinaryOperation("X", t1, t2)

            # Join in all other included tables
            while True:
                try:
                    child_op = BinaryOperation("X", child_op,
                                               self.tables_included.pop())
                except KeyError:
                    break

        # Nest child operation
        if self.child:
            child_op = BinaryOperation(self.join_operator.upper(), child_op,
                                       self.child.relational_algebra)

        # Insert RESTRICT operation
        if restrict_op:
            restrict_op.target = child_op
            project_op.target = restrict_op
        else:  # No RESTRICT operation
            project_op.target = child_op

        return project_op

    @property
    def query_tree(self):
        """ Getter for query tree. """
        return OpNode(self.relational_algebra)


def next_token():
    """ Generator function for collecting whitespace-separated tokens """
    global token
    for line in sys.stdin:
        for token in line.split():
            yield token


token_gen = next_token()

root_query = None
curr_query = None


def get_token():
    """ Wrapper function that lowers and iterates on generator object """
    global token
    token = next(token_gen).lower()
    return token
    

def is_aggregate():
    """ Parses to determine if following block is an aggregate function """
    global token

    mod_token = token.strip(',')
    if mod_token in AGGREGATE_FUNCTIONS:
        get_token()
        if token[0] == '(' and token[-1] == ')':  # Check for term
            token = token[1:-1]  # Remove surrounding parentheses
            if is_item():
                get_token()
                if token == "as":
                    get_token()
                    # TODO: Check for conflict with other identifiers
                    return token.isalnum()
                else:
                    return True
    return False
    
     
def is_attribute(manual_token=None):
    """ Parses to confirm that a token is an attribute """
    attr_token = manual_token if manual_token else token.strip(',')
    # Check if referring to specified table
    if len(attr_token.split('.')) > 1:

        table, item = attr_token.strip(',').split('.')
        # Table exists in schema, i.e. not aliased
        if table in SCHEMA.keys():
            return item == "*" or item in SCHEMA[table]

        # Table does not exist in schema, i.e. is aliased
        else:
            alias = table  # More idiomatic name
            # Attribute exists in schema
            if item not in COLUMNS:
                return False

            # Set of tables that include this item
            potential_tables = set([name for name, table in SCHEMA.items()
                                    if item in table])
            # TODO: Should only require one table. Potential to require many
            # Add set of potential tables to previous set of required tables
            if alias in curr_query.table_aliases_needed:
                curr_query.table_aliases_needed[alias] = curr_query \
                    .table_aliases_needed[alias] \
                    .intersection(potential_tables)
            # No previous set. New assignment.
            else:
                curr_query.table_aliases_needed[alias] = potential_tables
            return True

    # Not referred by table, not an aggregate function.
    elif attr_token not in AGGREGATE_FUNCTIONS:  # Not referred by table
        item = token.strip(',').strip(')')
        item_is_value = item.isnumeric() or (item[0] == "'" and item[-1] == "'")

        # Check item matches a column name
        if item in COLUMNS:
            potential_tables = [name for name, table in SCHEMA.items()
                                if item in table]
            if len(potential_tables) != 1:
                return False
            else:
                curr_query.tables_needed.add(potential_tables[0])
                return True
        else:
            # Wildcard and values are attributes
            return item == "*" or item_is_value

    # Break if no match
    else:
        return False
     

def is_condition():
    """ Parses to determine if the following block is a valid condition"""
    global condition
    global token

    # checks if operator is not separated by whitespace
    if is_operation():
        try:
            get_token()
        except StopIteration:
            return True

        if token in JOIN_OPERATIONS:
            return True
        elif token == "and" or token == "or":
            curr_query.condition_str += " " + token
            get_token()
            return is_condition()
        else:
            return False

    # whitespace after the first attribute
    elif is_attribute():
        condition["lhs"] = token
        get_token()
        # Matches a comparator
        if token in COMPARATOR_OPERATIONS:
            # surround IN keyword or any nested query with spaces
            if token == "in":
                curr_query.join_operator = "in"
                condition["op"] = " " + token + " "
            # normal assignment without IN
            else:
                condition["op"] = token

            get_token()
            split_token = token.split("(")
            condition["rhs"] = token

            c = condition

            if c["op"] == " in ":  # Operation is IN
                curr_query.condition_str += " " + c["lhs"]
            else:  # Standard operation
                curr_query.condition_str += " " + c["lhs"] + c["op"] + c["rhs"]

            # Checks if attribute or value
            if is_attribute() or (token[0] == "'" and token[-1] == "'"):
                try:
                    get_token()
                except StopIteration:
                    return True

                # Junction operator
                if token == "and" or token == "or":
                    curr_query.condition_str += " " + token
                    get_token()
                    return is_condition()

                # Join operator or End parentheses only
                return (token in JOIN_OPERATIONS) or (token.strip(')') == "")
            # checks if the next token is the start of a nested query
            elif len(split_token) > 1:
                # Begins a SELECT
                if split_token[-1] == "SELECT":
                    token = token[-1]
                # All parentheses
                elif split_token[-1] == '':
                    get_token()
                return is_query()
            # not attribute, value, or query -> not valid term
            else:
                return False
        else:
            split_token = token.split("(")
            # Begins a SELECT
            if split_token[-1] == "SELECT":
                token = token[-1]
            # All parentheses
            elif split_token[-1] == '':
                get_token()
            return is_query()

    elif is_aggregate():  # TODO: Add relational algebra stuff
        if token in COMPARATOR_OPERATIONS:
            # TODO: Left-hand side should be whole aggregate
            condition["op"] = token
            get_token()
            condition["rhs"] = token

            c = condition
            if c["op"] == "in":  # Operation is IN
                curr_query.condition_str += " " + c["lhs"]
            else:  # Standard operation
                curr_query.condition_str += " " + c["lhs"] + c["op"] + c["rhs"]
            return True

    # Catch all fail
    return False
    

def is_field():
    """ Parses to determine if following block is a valid field """
    return is_attribute()
        
      
def is_field_list():
    """ Parses to determine if following block is a valid list of fields """
    if not is_field():
        return False

    more_fields = token[-1] == ','
    try:
        get_token()
    except StopIteration:
        return not more_fields

    return not (more_fields and not is_field_list())


def is_item():
    """ Parses to determine if following block is an attribute item """
    if not is_attribute() and not is_aggregate():
        return False
    # Add item to project_needed, i.e. list of used attributes
    curr_query.project_needed.add(token.strip(","))
    return True


def is_items():
    """ Parses to determine if following block is a list of attribute items """
    if not is_item():
        return False

    # Check for further list of items
    if token[-1] == ',':  # List continues
        get_token()
        return is_items()
    else:
        return True


def is_operation():
    """ Parses to determine if current token is an operation """
    global condition

    try:  # Should only match one.
        operator = next(op for op in COMPARATOR_OPERATIONS
                        if op in token and op != "in")
    except StopIteration:
        return False

    if operator in token:
        lhs, rhs = token.split(operator)

        condition["lhs"] = lhs
        condition["rhs"] = rhs
        condition["op"] = operator

        # Add operation to condition string
        if condition["op"] != "in":
            curr_query.condition_str += " " + lhs + operator + rhs

        # Check that operation valid
        rhs_is_value = rhs[0] == "'" and rhs[-1] == "'"
        rhs_is_valid = is_attribute(rhs) or rhs_is_value or rhs.isnumeric()
        if not is_attribute(lhs) or not rhs_is_valid:
            return False

    return True


def is_query():
    """ Parses to determine if following block is a query """
    global curr_query
    global root_query
    global token

    # Generate and track tree structure
    curr_query = Query(parent=curr_query)
    if root_query is None:
        root_query = curr_query
    if curr_query.parent is not None:
        curr_query.parent.child = curr_query

    # Query starts with SELECT [DISTINCT]
    token = token.lstrip('(')
    if token != "select":
        return False
    get_token()
    if token == "distinct":
        get_token()

    # A list of items must follow
    if not is_items():
        return False

    # FROM table[ AS identifier][, table [AS identifier]]*
    if token != "from":  # Did not progress token from is_items()
        get_token()
    if token != "from":
        return False
    get_token()
    if not is_table_list():
        return False

    if token == "where":
        get_token()
        if not is_condition():
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    # TODO: Add group by to our parser
    # http://www.cbcb.umd.edu/confcour/Spring2014/CMSC424/Relational_algebra.pdf
    if token == "group":
        get_token()
        if token != "by":
            return False
        get_token()

        if not is_field_list():
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    if token == "having":
        get_token()
        if not is_condition():
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    if token == "order":
        get_token()  # by
        # TODO: Confirm that token is BY
        get_token()
        if not is_field_list():
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    if token == "union":
        curr_query.join_operator = token
        get_token()
        if not is_query():
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    if token == "intersect":
        curr_query.join_operator = token

        get_token()
        if not is_query():
            return False

        try:
            get_token()
        except StopIteration:
            return True

    if token == "except":
        curr_query.join_operator = "difference"

        get_token()
        if not is_query():
            return False

        try:
            get_token()
        except StopIteration:
            return True

    if token == "contains":
        curr_query.join_operator = token
        get_token()
        if not is_query():
            return False

        try:
            get_token()
        except StopIteration:
            return True

    if iter_stopped:
        return True

    return False  # Too many extra things


def is_table():
    """ Determines if the Table is valid """
    # NOTE: if match, proceeds the token because a check for alias exists.
    table_name = token.strip(',')
    # Table must be in schema
    if table_name not in TABLES:
        return False

    # Track tables included
    curr_query.tables_included.add(table_name)
    if token[-1] == ',':  # If part of a list, end
        return True

    try:
        get_token()
    except StopIteration:  # End of query - matches.
        return True

    # Check for join operations
    if token in JOIN_OPERATIONS:
        return True
    # Check for alias
    elif token == "as":
        get_token()
        if token in JOIN_OPERATIONS:
            return False

    # AS keyword is not required for an alias. for SOME REASON.
    stripped_token = token.strip(',')
    # Q?: Table name should not start with numbers?
    if stripped_token.isalnum():  # Q?: Check if identifier conflicts??
        # Current token should be the new alias
        if stripped_token not in curr_query.table_aliases_appeared:
            curr_query.table_aliases_appeared[stripped_token] = table_name
            return True
    return False


def is_table_list():
    """ Parses to determine if following block is a list of tables """
    global iter_stopped
    if not is_table():
        return False
    more_tables = bool(token[-1] == ',')  # Cast not necessary, just clarity

    # Q?: Why is this here if confirmed token is a table?
    if token not in JOIN_OPERATIONS and (token != "from" and token != "where"):
        try:
            get_token()
        except StopIteration:
            iter_stopped = True
            return not more_tables
    # Q?: Shouldn't there be an `else' statement to return false if it fails?

    # List continues
    return not (more_tables and not is_table_list())


if __name__ == "__main__":
    get_token()
    if is_query():
        print_tree(root_query.query_tree)
    else:
        print("Failed")
