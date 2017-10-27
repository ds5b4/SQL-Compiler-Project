#import sqltree.py 
import sys
from collections import namedtuple
from sqlRAlg import *


# TODO: Add check that all needed aliases appear and with correct table.

AGGREGATE_FUNCTIONS = ["avg", "max", "count"]  # TODO: Avg should be ave
SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}
COLUMNS = [ column for _, table in SCHEMA.items() for column in table ]
TABLES = [ table for table, _ in SCHEMA.items() ]


token = ""
count = 0

tables_included = set()
tables_needed = set()   # TODO: Find better place

project_needed = set()
selects_needed = set()
aggregates_needed = set()

table_aliases_needed = dict()
table_aliases_appeared = dict()

condition = {"lhs": '', "rhs": '', "operator": ''}
condition_string = ""


def next_token():
    """ Generator function for collecting whitespace-separated tokens """
    for line in sys.stdin:
        for token in line.split():
            yield token

            
token_gen = next_token()
def get_token():
    """ Wrapper function that lowers and iterates on generator object """
    global token
    token = next(token_gen).lower()
    print(token)
    return token
    
    
def is_query():
    """ Parses to determine if following block is a query """

    #

    # SELECT DISTINCT item[, item]*
    if token.lower() != "select":
        print("Not query because not SELECT at start")
        return False
    get_token()
    if token.lower() == "distinct":
        get_token()
    if not is_items():
        print("Not query because not items after select")
        return False
    #Create root node for this query section(will differ for )


    # FROM table[ AS identifier][, table [AS identifier]]*
    get_token()
    if token.lower() != "from":
        print("Not query because no FROM after select")
        return False
    get_token()  # TODO: We are here.
    if not is_table_list():
        print("Not query because no tables after FROM")
        return False

    # print("Should be before WHERE/GROUP/HAVING/ORDER: %s" % token)
    # # WHERE, GROUP BY, HAVING, ORDER BY
    # try:
    #     get_token()
    # except StopIteration:
    #     print("Is query because terminated without failing")
    #     return True

    if token.lower() == "where":
        get_token()
        if not is_condition():
            print("is_query: WHERE: token was not condition")
            return False
        
        try:
            get_token()
        except StopIteration:
            return True
    
    if token.lower() == "group":
        get_token()  # by 
        get_token()
        if not is_field_list():
            print("is_query: GROUP: token was not field list")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token.lower() == "having":
        get_token()
        if not is_condition():
            print("is_query: HAVING: token is not condition")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token.lower() == "order":
        get_token()  # by 
        get_token()
        if not is_field_list():
            print("is_query: ORDER: token was not field list")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True

    if token.lower() == "union":
        get_token()
        if not is_query():
            print("UNION: token not is_query")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    if token.lower() == "intersect":
        get_token()
        if not is_query():
            print("INTERSECT: token is not query")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
     
    if token.lower() == "except":
        get_token()
        if not is_query():
            print("EXCEPT token is not query")
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    
    # TODO: Continue here
    print("Failed: No case matched")
    return False  # Too many extra things

    
def is_aggregate():
    """ Parses to determine if following block is an aggregate function """
    mod_token = token.strip(',')
    if mod_token in AGGREGATE_FUNCTIONS:
        get_token()
        if token[0] == '(' and token[-1] == ')':
            token = token[1:-1]
            if is_items():
                get_token()
                if token.lower() == "as":
                    get_token()
                    if token.isalnum():  # TODO: Check for conflict with other identifiers
                        return True
    return False
    
     
def is_attribute(manual_token=None):
    attr_token = manual_token if manual_token else token.strip(',')

    # Check if referring to specified table
    if len(attr_token.split('.')) > 1:
        table, item = attr_token.strip(',').split('.')
        if table in SCHEMA.keys():  # Not aliased
            return(item == "*" or item in SCHEMA[table])
            
        else:  # Aliased
            alias = table

            if item in COLUMNS:
                potential_tables = set([ name for name, table in SCHEMA.items() if item in table ])

                if alias in table_aliases_needed:
                    table_aliases_needed[alias] = table_aliases_needed[alias].intersection(potential_tables) 
                else:
                    table_aliases_needed[alias] = potential_tables
                return True
            else:  # TODO: Attribute does not exist in schema
                return False
             
        
    elif attr_token not in AGGREGATE_FUNCTIONS:  # Not referred by table
        item = token.strip(',')
        
        # Check table membership
        if item in COLUMNS:
            tables = [ name for name, table in SCHEMA.items() if item in table ]
            if len(tables) == 1:
                tables_needed.put(tables)  # Track for later FROM clause
                return True
            else:
                return False  # TODO: Ambiguous attribute
            
        elif item == "*":
            return True    
        else:
            return False  # TODO: message invalid item/attribute
            
    else:
        return False
     
     
def is_items():
    """ Parses to determine if following block is an attribute item """
    if not is_attribute() and not is_aggregate(): 
        print("%s is not attribute and is not aggregate" % token)
        return False

    #TODO: find a better place for this
    project_needed.add(token)        #Used to add the root node to the tree may add all tokens so probably should change locations but idk

    # Check for further list of items
    if token[-1] == ',':  # List continues
        get_token()
        return is_items()
    else:
        return True
    

def is_table():
    """ Determines if the Table is valid """
    if token.strip(',') not in TABLES:
        print("token %s not in %s" % (token, TABLES))
        return False
        
    table_name = token.strip(',')
    
    tables_included.add(table_name)
    if token[-1] == ',':
        return True

    try:  
        get_token()
    except StopIteration:  # End of query - matches.
        return True

    if token.lower() == "as":
        get_token()
    stripped_token = token.strip(',')
    # Table name should not start with numbers?
    if stripped_token.isalnum():  # Check if identifier conflicts??
        # Current token should be the alias. table_name is the name of the aliased table.
        if stripped_token in table_aliases_appeared:
            print("Stripped token %s already appeared in aliases" % stripped_token)
            return False  # Conflict in alias name
        else:
            table_aliases_appeared[stripped_token] = table_name
            return True
    print("Catch all fail")
    return False
            
            
def is_table_list():
    """ Parses to determine if following block is a list of tables """
    if not is_table():
        print("Not is_table_list because not _is_table: %s" % token)
        return False
    more_tables = token[-1] == ','
        
    try:
        get_token()
    except StopIteration:  # Will trigger on last table, if no WHERE/GROUP/ORDER
        print("Expected more tables: %s" % more_tables)
        return not more_tables  # False if dangling comma, True if no dangling comma
    
    if more_tables:  # List continues
        return is_table_list()
    else:
        return True
    
    
def is_condition():
    """ Parses to determine if the folowing block is a valid condition"""
    global condition
    global token
    global condition


    if is_operation():  #checks for the contdition where operator is not separated by whitespace
        try:
            get_token()
        except StopIteration:
            return True

        if(token.lower() == "group" or token.lower() == "having" or token.lower() == "order" or token.lower() == "contains"):
            return True
        else:
            print("is_condition: improper end")
            return False

    elif(is_attribute(token)): #whitespace after the first attribute
        condition["lhs"] = token
        get_token()
        if(token == ">=" or token == "<=" or token == "!=" or token == "=" or token == ">" or token == "<" or token.lower() == "in"):
            #always a whitespace before and after IN keyword or any nested query
            condition["operator"] = token

            get_token()
            split_token = token.split("(")
            condition["rhs"] = token
            condition_string += condition["lhs"] + condition["operator"] + condition["rhs"]

            # Checks if attribute or value
            if is_attribute() or (token[0] == "'" and token[-1] == "'"):
                attr_val = token
                try:
                    get_token()
                except StopIteration:
                    return True

                if(token.lower() == "and" or token.lower() == "or"):
                    condition_string += " " + token + " "
                    # Get another token??
                    get_token()
                    is_condition()
                elif(token.lower() == "group" or token.lower() == "having" or token.lower() == "order" or token.lower() == "contains"):
                    return True
                else:
                    print("is_condition: binary op: after attribute/value, expected a conditional or an aggregate keyword")
                    return False
            #checks if the next token is the start of a nested query        
            elif len(split_token) > 1 :
                count += len(token - 1)
                if split_token[-1] == "SELECT":
                    token = token[-1]
                elif split_token[-1] == '':
                    get_token()
                return is_query()
            else: # not attribute, value, or query, meaning not valid term for a condition. Condition must be value or table.
                print("is_condition: binary op:  token not attribute, value, or query.")
                return False
        else:
            split_token = token.split("(")
            count += len(token - 1)
            if split_token[-1] == "SELECT":
                token = token[-1]
            elif split_token[-1] == '':
                get_token()
            print("is_attribute: Checking nested query")
            return is_query()


    else:
        print("is_condition: ELSE: %s" % token)
        return False
    

def is_operation():
    """ Parses the token to determine if the entire operator is in compacted without whitespace """
    global condition
    global condition_string

    # for operator in [">=", "<=" , "!=", "=" , ">", "<"]: #checks for no whitespace in a condition
    operator = next(op for op in [">=", "<=" , "!=", "=" , ">", "<"] if op in token)
    if operator in token:   
        lhs, rhs = token.split(operator)

        condition["lhs"] = lhs
        condition["rhs"] = rhs
        condition["operator"] = operator

        condition_string += condition["lhs"] + condition["operator"] + condition["rhs"]
        # Check that valid operation
        if not (is_attribute(lhs) and (is_attribute(rhs) or (rhs[0] == "'" and rhs[-1] == "'") or rhs.isnumeric())):
            print("is_operation: lhs not attribute and rhs not attribute, value, or numeric")
            return False
    
    try:
        get_token()
    except StopIteration:
        return True
    
    if(token.lower() == "and" or token.lower() == "or"):
        condition_string += " " + token + " "
        
        get_token()

        if is_condition():
            return True
        else:
            print("is_operation: not condition following AND/OR")
            return False

    return True


def is_field():
    """ Parses to determine if folllowing block is a valid field """
    return is_attribute()
        
      
def is_field_list():
    """ Parses to determine if following block is a valid list of fields """
    if not is_field():
        return False
    
    more_fields = token[-1] == ','
    try:
        get_token()
    except StopIteration:
        return more_fields
    
    if more_fields:
        return is_field_list()
    else:
        return True


def create_relational_algebra():
    for alias in table_aliases_needed:
        if alias not in table_aliases_needed:
            print("Alias not needed")
            return False

    if "*" in project_needed:  # Assuming wildcard only appears first, not after other columns
        projections = [column for table in tables_needed for column in SCHEMA[table] ]
        proj_op = UnaryOperation("PROJECT", None, projections)
    
    else:
        proj_op = UnaryOperation("PROJECT", None, project_needed)

    uni_op = None

    # operation, target, parameters=[], join_char=', '
    uni_op = UnaryOperation("RESTRICT", None, condition_string)

    if(len(table_aliases_appeared) != 0):
        def rename_table(table, alias):
            return "RENAME {1} ({0})".format(table, alias)

        assert len(table_aliases_appeared) != 0
        if len(table_aliases_appeared) == 1:
            bin_op = table_aliases_appeared.popitem()
        elif len(table_aliases_appeared) >= 2:
            alias, table = table_aliases_appeared.popitem()
            r1 = rename_table(table, alias)

            alias, table = table_aliases_appeared.popitem()
            r2 = rename_table(table, alias)

            bin_op = BinaryOperation("X", r1, r2)
        for alias, table in table_aliases_appeared.items():
            bin_op = BinaryOperation("X", bin_op, rename_table(table, alias))

    else:
        if len(tables_included) == 1:
            bin_op = tables_included.pop()
        elif len(tables_included) >= 2:
            bin_op = BinaryOperation("X", tables_included.pop(), tables_included.pop())
        try:
            while(True):
                bin_op = BinaryOperation("X", bin_op, tables_included.pop())
        except KeyError:
            pass


    if uni_op:
        uni_op.target = bin_op
        proj_op.target = uni_op
    else:
        proj_op.target = bin_op

    print(proj_op)
    return proj_op


def main():
    global token
    get_token()
    if is_query():
        create_relational_algebra()
    else:
        print("Failed")

    
    
if __name__=="__main__":
    main()