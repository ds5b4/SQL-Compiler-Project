#import sqltree.py 
import sys
from sqlRAlg import *

AGGREGATE_FUNCTIONS = ["avg", "max", "count"]  # TODO: Avg should be ave
SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}
COLUMNS = [ column for _, table in SCHEMA.items() for column in table ]
TABLES = [ table for table, _ in SCHEMA.items() ]
print("TABLES: %s" % TABLES)

token = ""
count = 0

tables_included = set()
tables_needed = set()   # TODO: Find better place

project_needed = set()
selects_needed = set()
aggregates_needed = set()
conditions_needed = set()
condition_constraints = set()
condition = []

table_aliases_needed = dict()
table_aliases_appeared = dict()


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

    # WHERE, GROUP BY, HAVING, ORDER BY
    try:
        get_token()
    except StopIteration:
        print("Is query because terminated without failing")
        return True
        
    if token.lower() == "where":
        get_token()
        if not is_condition():
            return False
        
        try:
            get_token()
        except StopIteration:
            return True
    
    if token.lower() == "group":
        get_token()  # by 
        get_token()
        if not is_field_list():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token.lower() == "having":
        get_token()
        if not is_condition():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token.lower() == "order":
        get_token()  # by 
        get_token()
        if not is_field_list():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True

    
    if token.lower() == "union":
        get_token()
        if not is_query():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    if token.lower() == "intersect":
        get_token()
        if not is_query():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    
    if token.lower() == "except":
        get_token()
        if not is_query():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    
    # TODO: Continue here
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
        # TODO: Create alias table
        table_aliases.add(table_name)
        if stripped_token in table_aliases_appeared:
            print("Stripped token %s already appeared in aliases")
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
    if is_operation():  #checks for the contdition where operator is not separated by whitespace
        get_token()
        conditions_needed.add(condition) #each element is a list where it is stored lhs, condtion, rhs
        condition = []
        if(token.lower() == "and" or token.lower() == "or"):
            condition_constraints.add(token) #used to store and/or values for conditions so they dont have to follow the same rules as conditions_needed
            get_token()
            is_condition()
        elif(token.lower() == "group" or token.lower() == "having" or token.lower() == "order" or token.lower() == "contains"):
            return True
        else:
            return False

    elif(is_attribute(token)): #whitespace after the first attribute
        condition.append(token)
        get_token()
        if(token == ">=" or token == "<=" or token == "!=" or token == "=" or token == ">" or token == "<" or token.lower() == "in"):
            #always a whitespace before and after IN keyword or any nested query
            condition.append(token)
            get_token()
            split_token = token.split("(")
            condition.append(token)
            if is_attribute() or (token[0] == "'" and token[-1] == "'"):
                if(token.lower() == "and" or token.lower() == "or"):
                    condition_constraints.add(token)
                    is_condition()
                elif(token.lower() == "group" or token.lower() == "having" or token.lower() == "order" or token.lower() == "contains"):
                    return True
                else:
                    return False
            #checks if the next token is the start of a nested query        
            elif len(split_token) > 1 :
                count += len(token - 1)
                if split_token[-1] == "SELECT":
                    token = token[-1]
                elif split_token[-1] == '':
                    get_token()
                return is_query()
            else: 
                return False
        else:
            split_token = token.split("(")
            count += len(token - 1)
            if split_token[-1] == "SELECT":
                token = token[-1]
            elif split_token[-1] == '':
                get_token()
            return is_query()

        conditions_needed.add(condition)

    else:
        return False
    

def is_operation():
    """ Parses the token to determine if the entire operator is in compacted without whitespace """
    for operator in [">=", "<=" , "!=", "=" , ">", "<"]: #checks for no whitespace in a condition
        if operator in token:   
            lhs, rhs = token.split()
            condition.append([lhs, operator, rhs])
            #TODO: This does not account for mismatched spacing(operator only attached to one of the attributes)
            return is_attribute(lhs) and (is_attribute(rhs) or (rhs[0] == "'" and rhs[-1] == "'"))    


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
    print(table_aliases_needed)
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
    if(len(conditions_needed) != 0):
        conditions_used = []
        for i in len(conditions_needed):
            conditions_used.append(conditions_needed[i][0] + conditions_needed[i][1] + conditions_needed[i][2])
        print("----")
        print(conditions_used)
        print(condition_constraints)
        uni_op = UnaryOperation("RESTRICT", None, conditions_used, condition_constraints)

    if(len(table_aliases_appeared) != 0):
        assert len(table_aliases_appeared != 0)
        if len(table_aliases_appeared == 1):
            bin_op = table_aliases_appeared[0]
        elif len(table_aliases_appeared >= 2):
            bin_op = BinaryOperation("X", table_aliases_appeared[0], table_aliases_appeared[1])
        for table in table_aliases_appeared[2]:
            bin_op = BinaryOperation("X", bin_op, table)

    else:
        print(tables_included)
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

def main():
    global token
    get_token()
    if is_query():
        create_relational_algebra()
    else:
        print("Failed")

    
    
if __name__=="__main__":
    main()