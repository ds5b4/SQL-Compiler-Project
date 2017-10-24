AGGREGATE_FUNCTIONS = ["avg", "max", "count"]  # TODO: Avg should be ave
SCHEMA = {
    "sailors": ["sid", "sname", "rating", "age"],
    "boats": ["bid", "bname", "color"],
    "reserves": ["sid", "bid", "day"]
}
COLUMNS = [ column for _, table in SCHEMA.items() for column in table ]
TABLES = [ table for _, table in SCHEMA.items() ]

token = ""
count = 0

tables_included = set()
tables_needed = set()   # TODO: Find better place

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
    return next(token_gen).lower()
    
    
def is_query():
    """ Parses to determine if following block is a query """
    # SELECT DISTINCT item[, item]*
    if token != "select":
        return False
    get_token()
    if token == "distinct":
        get_token()
    if not is_items():
        return False

    # FROM table[ AS identifier][, table [AS identifier]]*
    get_token()
    if token != "from":
        return False
    get_token()
    if not is_table_list():
        return False

    # WHERE, GROUP BY, HAVING, ORDER BY
    try:
        get_token()
    except StopIteration:
        return True
        
    if token == "where":
        get_token()
        if not is_condition():
            return False
        
        try:
            get_token()
        except StopIteration:
            return True
    
    if token == "group":
        get_token()  # by 
        get_token()
        if not is_field_list():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token == "having":
        get_token()
        if not is_condition():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
            
    if token == "order":
        get_token()  # by 
        get_token()
        if not is_field_list():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True

    
    if token == "union":
        get_token()
        if not is_query():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    if token == "intersect":
        get_token()
        if not is_query():
            return False
            
        try:
            get_token()
        except StopIteration:
            return True
    
    
    if token == "except":
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
                if token == "AS":
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
        return False
    
    get_token()
    # Check for further list of items
    if token[-1] == ',':  # List continues
        return is_items()
    else:
        return True
    

def is_table():
    if token.strip(',') not in TABLES:
        return False
        
    table_name = token.strip(',')
        
    if token[-1] == ',':
        tables_included.add(table_name)
        return True
        
    get_token()
    if token == "AS":
        get_token()
    stripped_token = token.strip(',')
    # Table name should not start with numbers?
    if stripped_token.isalnum():  # Check if identifier conflicts??
        # TODO: Create alias table
        if stripped_token in table_aliases_appeared:
            return False  # Conflict in alias name
        else:
            table_aliases_appeared[stripped_token] = table_name
            return True
    return False
            
            
def is_table_list():
    """ Parses to determine if following block is a list of tables """
    if not is_table():
        return False
        
    more_tables = token[-1] == ','
        
    try:
        get_token()
    except StopIteration:  # Will trigger on last table, if no WHERE/GROUP/ORDER
        return more_tables  # False if dangling comma, True if no dangling comma
    
    if more_tables:  # List continues
        return is_table_list()
    else:
        return True
    
    
def is_condition():
    """ Parses to determine if the folowing block is a valid condition"""
    if is_operation():  #checks for the contdition where operator is not separated by whitespace
        get_token()
        if(token == "AND" or token == "OR"):
            get_token()
            is_condition()
        elif(token == "GROUP" or token == "HAVING" or token == "ORDER" or token == "CONTAINS"):
            return True
        else:
            return False

    elif(is_attribute(token)): #whitespace after the first attribute
        get_token()
        if(token == ">=" or token == "<=" or token == "!=" or token == "=" or token == ">" or token == "<" or token == "IN"):
            #always a whitespace before and after IN keyword or any nested query
            get_token()
            split_token = token.split("(")

            if is_attribute() or (token[0] = "'" and token[-1] = "'"):
                if(token == "AND" or token == "OR"):
                    is_condition()
                elif(token == "GROUP" or token == "HAVING" or token == "ORDER" or token=="CONTAINS"):
                    return True
                else:
                    return False
            #checks if the next token is the start of a nested query        
            elif len(split_token) > 1 :
                count += len(token - 1)
                if split_token[-1] == "SELECT":
                    token = token[-1]
                elif split_token[-1] = '':
                    get_token()
                return is_query()
            else: 
                return False
        else:
            split_token = token.split("(")
            count += len(token - 1)
            if split_token[-1] == "SELECT":
                token = token[-1]
            elif split_token[-1] = '':
                get_token()
            return is_query()

    else:
        return False
    

def is_operation():
    """ Parses the token to determine if the entire operator is in compacted without whitespace """
    for operator in [">=", "<=" , "!=", "=" , ">", "<"] #checks for no whitespace in a condition
        if operator in token:   
            lhs, rhs = token.split()
            #TODO: This does not account for mismatched spacing(operator only attached to one of the attributes)
            return is_attribute(lhs) and (is_attribute(rhs) or (rhs[0] = "'" and rhs[-1] = "'"))    


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
    
    
def main():
    global token
    get_token()
    if is_query():
        print("Congratulations drinks")
    else:
        print("Suffering drinks")
    
    
if __name__=="__main__":
    pass