import sys
from sqlRAlg import *


# TODO: Add check that all needed aliases appear and with correct table.

AGGREGATE_FUNCTIONS = ["ave", "max", "count"]
SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}
COLUMNS = [column for _, table in SCHEMA.items() for column in table]
# noinspection PyRedeclaration
TABLES = [table for table, _ in SCHEMA.items()]


token = ""
count = 0

tables_included = set()
tables_needed = set()

project_needed = set()
selects_needed = set()
aggregates_needed = set()

table_aliases_needed = dict()
table_aliases_appeared = dict()

condition = {"lhs": '', "rhs": '', "operator": ''}
condition_string = ""


def next_token():
    """ Generator function for collecting whitespace-separated tokens """
    global token

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

    # Create root node for this query section(will differ for )

    # FROM table[ AS identifier][, table [AS identifier]]*
    get_token()
    if token.lower() != "from":
        print("Not query because no FROM after select")
        return False
    get_token()
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

    print("TOKEN: %s " % token)  # TODO: Delete when done testing sample3
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

    print("is_query: No case matched")
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
                if token.lower() == "as":
                    get_token()
                    if token.isalnum():
                        # TODO: Check for conflict with other identifiers
                        return True
                    else:
                        print("is_aggregate: %s is not alphanumeric" % token)
                else:
                    print("is_aggregate: expected `as`, got `%s`" % token)
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
            return item == "*" or item in SCHEMA[table]
            
        else:  # Aliased
            alias = table

            if item in COLUMNS:
                potential_tables = set([name
                                        for name, table
                                        in SCHEMA.items()
                                        if item in table])

                if alias in table_aliases_needed:
                    table_aliases_needed[alias] = table_aliases_needed[alias]\
                        .intersection(potential_tables)
                else:
                    table_aliases_needed[alias] = potential_tables
                return True
            else:
                print("is_attribute: item %s does not exist in schema" % item)
                return False

    elif attr_token not in AGGREGATE_FUNCTIONS:  # Not referred by table
        item = token.strip(',')
        
        # Check table membership
        if item in COLUMNS:
            tables = [name for name, table in SCHEMA.items() if item in table]
            if len(tables) == 1:
                tables_needed.add(tables[0])  # Track for later FROM clause
                return True
            else:
                print("is_attribute: attribute `%s` is ambiguous and exists in"
                      " tables %s" % (item, tables))
                return False
        elif item == "*":
            return True    
        else:
            print("is_attribute: `%s` not valid attribute or wildcard" % token)
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
    project_needed.add(token)

    # Check for further list of items
    if token[-1] == ',':  # List continues
        get_token()
        return is_items()
    else:
        return True
    

def is_table():
    """ Determines if the Table is valid """
    if token.strip(',') not in TABLES:
        print("is_table: token %s not in %s" % (token, TABLES))
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
        # Current token should be the alias
        # table_name is the name of the aliased table.
        if stripped_token in table_aliases_appeared:
            print("is_table: %s already appeared in aliases" % stripped_token)
            return False  # Conflict in alias name
        else:
            table_aliases_appeared[stripped_token] = table_name
            return True
    print("is_table: Catch all fail")
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
        return not more_tables  # dangling comma
    
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
    global condition_string
    global count
    global token

    if is_operation():  # checks if operator is not separated by whitespace
        try:
            get_token()
        except StopIteration:
            return True

        if token.lower() == "group" \
                or token.lower() == "having" \
                or token.lower() == "order" \
                or token.lower() == "contains":
            return True
        else:
            print("is_cond: is_operation: improper end")
            return False

    elif is_attribute():  # whitespace after the first attribute
        condition["lhs"] = token
        get_token()
        if token.lower() in ['>=', '<=', '!=', '=', '>', '<', 'in']:
            # always whitespaces around IN keyword or any nested query
            condition["operator"] = token

            get_token()
            split_token = token.split("(")
            condition["rhs"] = token

            c = condition
            condition_string += c["lhs"] + c["operator"] + c["rhs"]

            # Checks if attribute or value
            if is_attribute() or (token[0] == "'" and token[-1] == "'"):
                # attr_val = token
                try:
                    get_token()
                except StopIteration:
                    return True

                if token.lower() == "and" or token.lower() == "or":
                    condition_string += " " + token + " "
                    # Get another token??
                    get_token()
                    is_condition()
                elif token.lower() == "group" \
                        or token.lower() == "having" \
                        or token.lower() == "order" \
                        or token.lower() == "contains":
                    return True
                else:
                    print("is_cond: is_attr: is_attr or '': expected a "
                          "conditional or an aggregate keyword")
                    return False
            # checks if the next token is the start of a nested query
            elif len(split_token) > 1:
                count += len(token) - 1
                if split_token[-1] == "SELECT":
                    token = token[-1]
                elif split_token[-1] == '':
                    get_token()
                return is_query()
            else:  # not attribute, value, or query -> not valid term
                print("is_cond: is_attr: token not attribute, value, or query.")
                return False
        else:
            split_token = token.split("(")
            count += len(token) - 1
            if split_token[-1] == "SELECT":
                token = token[-1]
            elif split_token[-1] == '':
                get_token()
            if is_query():
                return True
            else:
                print("is_cond: expected nested query")
                return False

    else:
        print("is_cond: ELSE: %s not operation or attribute" % token)
        return False
    

def is_operation():
    """ Parses to determine if current token is an operation """
    global condition
    global condition_string

    # Should only ever match once. Use `next` instead of `for in`
    try:
        operator = next(op
                        for op in [">=", "<=", "!=", "=", ">", "<"]
                        if op in token)

    except StopIteration:
        print("is_operation: %s did not contain an operator" % token)
        return False
    if operator in token:   
        lhs, rhs = token.split(operator)

        condition["lhs"] = lhs
        condition["rhs"] = rhs
        condition["operator"] = operator
        condition_string += lhs + operator + rhs

        # Check that valid operation
        rhs_is_value = rhs[0] == "'" and rhs[-1] == "'"
        rhs_is_valid = is_attribute(rhs) or rhs_is_value or rhs.isnumeric()
        if not is_attribute(lhs) or not rhs_is_valid:
            print("is_operation: lhs not attr or rhs not valid")
            return False
    
    try:
        get_token()
    except StopIteration:
        return True
    
    if token.lower() == "and" or token.lower() == "or":
        condition_string += " " + token + " "
        
        get_token()

        if is_condition():
            return True
        else:
            print("is_operation: not condition following AND/OR")
            return False

    return True


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
        return more_fields
    
    if more_fields:
        if is_field_list():
            return True
        else:
            print("is_field_list: expected more fields")
            return False
    else:
        return True


def create_relational_algebra():
    """ Generates the relational algebra for the parsed query. """
    for alias in table_aliases_needed:
        if alias not in table_aliases_appeared:
            print("create_relational_algebra: required alias did not appear")
            return False

    if "*" in project_needed:
        # Assuming wildcard only appears first, not after other columns
        projections = [column
                       for table in tables_needed
                       for column in SCHEMA[table]]
        proj_op = UnaryOperation("PROJECT", None, projections)
    
    else:
        proj_op = UnaryOperation("PROJECT", None, project_needed)

    # operation, target, parameters=[], join_char=', '
    if len(condition_string) > 0:
        uni_op = UnaryOperation("RESTRICT", None, condition_string)
    else:
        uni_op = None

    # TODO: Might want to mix aliased and non-aliased.
    if len(table_aliases_appeared) != 0:  # if aliased tables
        def rename_table(table, al):
            """ Converts a table and its alias to appropriate RENAME / RHO
            relational algebra.
            """
            return UnaryOperation("RENAME", table, al)

        if len(table_aliases_appeared) < 1:
            raise ValueError
        elif len(table_aliases_appeared) == 1:
            bin_op = table_aliases_appeared.popitem()
        else:  # len(table_aliases_appeared) >= 2:
            alias, table = table_aliases_appeared.popitem()
            r1 = rename_table(table, alias)

            alias, table = table_aliases_appeared.popitem()
            r2 = rename_table(table, alias)

            bin_op = BinaryOperation("X", r1, r2)
        for alias, table in table_aliases_appeared.items():
            bin_op = BinaryOperation("X", bin_op, rename_table(table, alias))

    else:  # Not aliased
        if len(tables_included) < 1:
            raise ValueError
        elif len(tables_included) == 1:
            bin_op = tables_included.pop()
        else:  # if len(tables_included) >= 2:
            t1 = tables_included.pop()
            t2 = tables_included.pop()
            bin_op = BinaryOperation("X", t1, t2)

        # Join in all other included tables
        while True:
            try:
                bin_op = BinaryOperation("X", bin_op, tables_included.pop())
            except KeyError:
                break

    if uni_op:
        uni_op.target = bin_op
        proj_op.target = uni_op
    else:
        proj_op.target = bin_op

    print(proj_op)
    return proj_op


if __name__ == "__main__":
    get_token()
    if is_query():
        create_relational_algebra()
    else:
        print("Failed")
