# coding=utf-8

"""
Authors: Maruska, John; Strickland, David
Class: CS5300 - Database Systems
Project: SQL Compiler Project

Script takes in a SQL query through standard input, generates its relational
algebra, and prints out the result. Created for Missouri S&T CS5300 - Database
Systems semester project.
"""

import sys
from sqlRAlg import *


# TODO: Add check that all needed aliases appear and with correct table.

# TODO: Relational algebra for following samples: 3, 5, 6, 7, 10

# Note for the report:
# TODO: sample 12, Boats.name does not exist, but Boats.bname does.
# TODO: Sample 9 is supposed to break on the following:
#   missing AS between Sailors and S2
#   missing aliased table R
#   three part equivalence check

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

condition = {"lhs": '', "rhs": '', "operator": ''}
iter_stopped = False
token = ""


class Query:
    """ Wrapping class that stores all query data such as tables, conditions,
    etc., as well as handling logic around the query such as relational algebra
    generation. Created so that queries can be easily nested.
    """
    def __init__(self, parent=None, child=None, join_operator=None):
        self.parent = parent
        self.child = child
        self.join_operator = join_operator

        self.tables_included = set()
        self.tables_needed = set()
        self.project_needed = set()
        self.selects_needed = set()
        self.aggregates_needed = set()

        self.table_aliases_needed = dict()
        self.table_aliases_appeared = dict()

        self.condition_str = ""

    def __str__(self):
        return self.generate_relational_algebra()

    @property
    def relational_algebra(self):
        """ String of relational algebra representing query """
        return self.generate_relational_algebra()

    def generate_relational_algebra(self):
        """ Generates the relational algebra for the parsed query. """
        for alias in self.table_aliases_needed:
            if alias not in self.table_aliases_appeared:
                print(
                    "create_rel_alg: required alias %s did not appear" % alias)
                return False

        if "*" in self.project_needed:
            # Assuming wildcard only appears first, not after other columns
            projections = [column for table in self.tables_included
                           for column in SCHEMA[table]]
            proj_op = UnaryOperation("PROJECT", None, projections)

        else:
            proj_op = UnaryOperation("PROJECT", None, self.project_needed)

        # operation, target, parameters=[], join_char=', '
        if len(self.condition_str) > 0:
            uni_op = UnaryOperation("RESTRICT", None, self.condition_str)
        else:
            uni_op = None

        # TODO: Might want to mix aliased and non-aliased.
        if len(self.table_aliases_appeared) != 0:  # if aliased tables
            def rename_table(table, al):
                """ Converts a table and its alias to appropriate RENAME / RHO
                relational algebra.
                """
                return UnaryOperation("RENAME", table, al)

            if len(self.table_aliases_appeared) < 1:
                raise ValueError
            elif len(self.table_aliases_appeared) == 1:
                bin_op = self.table_aliases_appeared.popitem()
            else:  # len(table_aliases_appeared) >= 2:
                alias, table = self.table_aliases_appeared.popitem()
                r1 = rename_table(table, alias)

                alias, table = self.table_aliases_appeared.popitem()
                r2 = rename_table(table, alias)

                bin_op = BinaryOperation("X", r1, r2)
            for alias, table in self.table_aliases_appeared.items():
                bin_op = BinaryOperation("X", bin_op,
                                         rename_table(table, alias))

        else:  # Not aliased
            if len(self.tables_included) < 1:
                raise ValueError
            elif len(self.tables_included) == 1:
                bin_op = self.tables_included.pop()
            else:  # if len(tables_included) >= 2:
                t1 = self.tables_included.pop()
                t2 = self.tables_included.pop()
                bin_op = BinaryOperation("X", t1, t2)

            # Join in all other included tables
            while True:
                try:
                    bin_op = BinaryOperation("X", bin_op,
                                             self.tables_included.pop())
                except KeyError:
                    break

        if uni_op:
            uni_op.target = bin_op
            proj_op.target = uni_op
        else:
            proj_op.target = bin_op

        return proj_op


def next_token():
    """ Generator function for collecting whitespace-separated tokens """
    global token
    for line in sys.stdin:
        for token in line.split():
            yield token


token_gen = next_token()
root_query = Query()
current_query = root_query


def get_token():
    """ Wrapper function that lowers and iterates on generator object """
    global token
    token = next(token_gen).lower()
    # print(token)
    return token
    
    
def is_query():
    """ Parses to determine if following block is a query """
    global token
    token = token.lstrip('(')
    if token != "select":
        print("is_query: %s not SELECT at start" % token)
        return False
    get_token()
    if token == "distinct":
        get_token()

    if not is_items():
        print("Not query because not items after select")
        return False

    # FROM table[ AS identifier][, table [AS identifier]]*
    if token != "from":
        get_token()

    if token != "from":
        print("is_query: expected FROM, got %s" % token)
        return False

    get_token()
    if not is_table_list():
        print("Not query because no tables after FROM")
        return False

    if token == "where":
        get_token()
        if not is_condition():
            print("is_query: WHERE: token was not condition")
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True
    
    if token == "group":
        get_token()  # by
        get_token()
        if not is_field_list():
            print("is_query: GROUP: token was not field list")
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True
            
    if token == "having":
        get_token()
        if not is_condition():
            print("is_query: HAVING: token is not condition")
            return False

        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True
            
    if token == "order":
        get_token()  # by 
        get_token()
        if not is_field_list():
            print("is_query: ORDER: token was not field list")
            return False
            
        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True

    if token == "union":
        print("UNION TOKEN REGISTERED")
        get_token()
        if not is_query():
            print("UNION: token not is_query")
            return False
        else:
            print("Successfully confirmed query")
            
        if token not in JOIN_OPERATIONS:
            try:
                get_token()
            except StopIteration:
                return True
    
    if token == "intersect":
        get_token()
        if not is_query():
            print("INTERSECT: token is not query")
            return False
        else:
            print("INTERSECT: successfully got query")
            
        try:
            get_token()
        except StopIteration:
            return True
     
    if token == "except":
        get_token()
        if not is_query():
            print("EXCEPT token is not query")
            return False
        else:
            print("EXCEPT: successfully got query")
            
        try:
            get_token()
        except StopIteration:
            return True

    if token == "contains":
        get_token()
        if not is_query():
            print("CONTAINS token is not query")
            return False

        try:
            get_token()
        except StopIteration:
            return True

    if iter_stopped:
        return True

    print("is_query: Either no case matched, or extra tokens")
    return False  # Too many extra things

    
def is_aggregate():
    """ Parses to determine if following block is an aggregate function """
    global token

    mod_token = token.strip(',')
    if mod_token in AGGREGATE_FUNCTIONS:
        get_token()
        if token[0] == '(' and token[-1] == ')':
            token = token[1:-1]
            if is_items():
                get_token()
                if token == "as":
                    get_token()
                    if token.isalnum():
                        # TODO: Check for conflict with other identifiers
                        return True
                    else:
                        print("is_aggregate: %s is not alphanumeric" % token)
                else:
                    return True
            else:
                print("is_aggregate: expected items")
        else:
            print("is_aggregate: expected paren-enclosed token. Got %s" % token)
    else:
        print("is_aggregate: `%s` not in %s" % (mod_token, AGGREGATE_FUNCTIONS))
    return False
    
     
def is_attribute(manual_token=None):
    """ Parses to confirm that a token is an attribute """
    attr_token = manual_token if manual_token else token.strip(',')
    # Check if referring to specified table
    if len(attr_token.split('.')) > 1:

        table, item = attr_token.strip(',').split('.')
        if table in SCHEMA.keys():  # Not aliased
            if item == "*" or item in SCHEMA[table]:
                return True
            else:
                print("is_attribute: %s is not * or in %s attributes %s" %
                      (item, table, SCHEMA[table]))
            return item == "*" or item in SCHEMA[table]
            
        else:  # Aliased
            alias = table

            if item in COLUMNS:
                potential_tables = set([name for name, table in SCHEMA.items()
                                        if item in table])

                if alias in current_query.table_aliases_needed:
                    current_query.table_aliases_needed[alias] = current_query \
                        .table_aliases_needed[alias] \
                        .intersection(potential_tables)
                else:
                    current_query.table_aliases_needed[alias] = potential_tables
                return True
            else:
                print("is_attribute: %s does not exist in schema" % item)
                return False

    elif attr_token not in AGGREGATE_FUNCTIONS:  # Not referred by table
        item = token.strip(',').strip(')')
        item_is_value = item.isnumeric() or (item[0] == "'" and item[-1] == "'")

        # Check table membership
        if item in COLUMNS:
            tables = [name for name, table in SCHEMA.items() if item in table]
            if len(tables) == 1:
                # Track for later FROM clause
                current_query.tables_needed.add(tables[0])
                return True
            else:
                print("is_attribute: attribute `%s` is ambiguous and exists in"
                      " tables %s" % (item, tables))
                return False
        elif item == "*" or item_is_value:
            return True
        else:
            print("is_attribute: %s not valid attr, val, or *" % token)
            return False
            
    else:
        return False
     
     
def is_items():
    """ Parses to determine if following block is an attribute item """
    if not is_attribute() and not is_aggregate(): 
        print("is_items: %s is not attribute and is not aggregate" % token)
        return False
    # Used to add the root node to the tree may add all tokens
    # probably should change locations but idk to where
    current_query.project_needed.add(token)

    # Check for further list of items
    if token[-1] == ',':  # List continues
        get_token()
        return is_items()
    else:
        return True
    

def is_table():
    """
    Determines if the Table is valid
    NOTE: if match, proceeds the token because a check for alias exists.
    """
    if token.strip(',') not in TABLES:
        print("is_table: token %s not in %s" % (token, TABLES))
        return False
        
    table_name = token.strip(',')
    
    current_query.tables_included.add(table_name)
    if token[-1] == ',':
        return True

    try:
        get_token()
    except StopIteration:  # End of query - matches.
        return True

    if token == "as":
        get_token()

    if token in JOIN_OPERATIONS:
        return True

    stripped_token = token.strip(',')
    # Table name should not start with numbers?
    if stripped_token.isalnum():  # Check if identifier conflicts??
        # Current token should be the alias
        # table_name is the name of the aliased table.
        if stripped_token in current_query.table_aliases_appeared:
            print("is_table: %s already appeared in aliases" % stripped_token)
            return False  # Conflict in alias name
        else:
            current_query.table_aliases_appeared[stripped_token] = table_name
            return True
    print("is_table: Catch all fail")
    return False
            
            
def is_table_list():
    """ Parses to determine if following block is a list of tables """
    global iter_stopped
    if not is_table():
        print("Not is_table_list because not is_table: %s" % token)
        return False
    more_tables = token[-1] == ','

    if token not in JOIN_OPERATIONS or token != "from":
        try:
            get_token()
        except StopIteration:
            iter_stopped = True
            if more_tables:
                print("Expected more tables")
                return False
            else:
                return True
    
    if more_tables:  # List continues
        if is_table_list():
            return True
        else:
            print("is_table_list: expected list to continue")
            return False
    else:
        return True
    
    
def is_condition():
    """ Parses to determine if the following block is a valid condition"""
    global condition
    global token

    if is_operation():  # checks if operator is not separated by whitespace
        try:
            get_token()
        except StopIteration:
            return True

        if token in JOIN_OPERATIONS:
            return True

        if token == "and" or token == "or":
            current_query.condition_str += " " + token + " "

            get_token()
            if is_condition():
                return True
            else:
                print("is_cond: is_op: not condition following AND/OR")
                return False

        else:
            print("is_cond: is_op: improper end")
            return False

    elif is_attribute():  # whitespace after the first attribute
        condition["lhs"] = token
        get_token()
        if token in COMPARATOR_OPERATIONS:
            # always whitespaces around IN keyword or any nested query
            condition["operator"] = token

            get_token()
            split_token = token.split("(")
            condition["rhs"] = token

            c = condition
            current_query.condition_str += c["lhs"] + c["operator"] + c["rhs"]

            # Checks if attribute or value
            if is_attribute() or (token[0] == "'" and token[-1] == "'"):
                # attr_val = token
                try:
                    get_token()
                except StopIteration:
                    return True

                if token == "and" or token == "or":
                    current_query.condition_str += " " + token + " "
                    # Get another token??
                    get_token()
                    if is_condition():
                        return True
                    else:
                        print("is_cond: is_attr: is_attr/'': condition to "
                              "follow and/or")
                        return False
                elif token in JOIN_OPERATIONS:
                    return True
                elif token.strip(')') == "":
                    return True
                else:
                    print("is_cond: is_attr: is_attr or val: expected a "
                          "conditional or an aggregate keyword. Got %s" %
                          token)
                    return False
            # checks if the next token is the start of a nested query
            elif len(split_token) > 1:
                if split_token[-1] == "SELECT":
                    token = token[-1]
                elif split_token[-1] == '':
                    get_token()
                if is_query():
                    print("Successfully got query - line 406")
                    return True
                else:
                    print("Failed to get query - line 409")
                    return False

            else:  # not attribute, value, or query -> not valid term
                print("is_cond: is_attr: token not attribute, value, or query.")
                return False
        else:
            split_token = token.split("(")
            if split_token[-1] == "SELECT":
                token = token[-1]
            elif split_token[-1] == '':
                get_token()
            if is_query():
                print("Successfully got query: line 423")
                return True
            else:
                print("is_cond: expected nested query")
                return False

    elif is_aggregate():  # TODO: Add relational algebra stuff
        if token in COMPARATOR_OPERATIONS:
            # TODO: Left-hand side should be whole aggregate
            condition["operator"] = token

            get_token()
            condition["rhs"] = token

            c = condition
            current_query.condition_str += c["lhs"] + c["operator"] + c["rhs"]
            return True
        else:
            print("is_cond: is_aggr: %s not in %s" %
                  (token, COMPARATOR_OPERATIONS))
            return False

    else:
        print("is_cond: ELSE: %s not operation or attribute" % token)
        return False
    

def is_operation():
    """ Parses to determine if current token is an operation """
    global condition

    try:
        operator = next(op for op in COMPARATOR_OPERATIONS
                        if op in token and op != "in")
    except StopIteration:
        # print("is_operation: %s did not contain an operator" % token)
        return False

    if operator in token:   
        lhs, rhs = token.split(operator)

        condition["lhs"] = lhs
        condition["rhs"] = rhs
        condition["operator"] = operator
        current_query.condition_str += lhs + operator + rhs

        # Check that valid operation
        rhs_is_value = rhs[0] == "'" and rhs[-1] == "'"
        rhs_is_valid = is_attribute(rhs) or rhs_is_value or rhs.isnumeric()
        if not is_attribute(lhs) or not rhs_is_valid:
            print("is_operation: lhs not attr or rhs not valid")
            return False

    return True


def is_field():
    """ Parses to determine if following block is a valid field """
    return is_attribute()
        
      
def is_field_list():
    """ Parses to determine if following block is a valid list of fields """
    if not is_field():
        print("is_field_list: %s was not field" % token)
        return False

    more_fields = token[-1] == ','
    try:
        get_token()
    except StopIteration:
        if not more_fields:
            return True
        else:
            print("is_field_list: Expected more fields")
            return False
    
    if more_fields:
        if is_field_list():
            return True
        else:
            print("is_field_list: expected more fields")
            return False
    else:
        return True


if __name__ == "__main__":
    get_token()
    if is_query():
        print(root_query.relational_algebra)
    else:
        print("Failed")
