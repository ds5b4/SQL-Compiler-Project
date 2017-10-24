class Node():
    def __init__(parent=None, children=[]):
        self.parent = parent
        
        if parent not None:
            self.depth = parent.depth + 1
        else:
            self.depth = 0
            
        # TODO: Assert children not larger than 2. Binary or unary operations only
        self.children = [child for child in children]
        
    def is_root():
        return self.parent is None

            
class LeafNode(Node):
    def __init__(table, parent=None, children=None)
        super.__init__(parent, children)
        
        self.table = table
        
        
class OperatorNode(Node):
    def __init__(operator, children, param_str=None, parent=None)
        super.__init__(parent, children)
        
        self.operator = operator
        self.children = [child for child in children]
        
        
class RelationalTree():
    def __init__(query):
        self.root = None
        self.query = query
        
        
    def parse_query():
        raise NotImplementedError