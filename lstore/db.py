from lstore.table import Table
from lstore.bufferpool import BufferPool
import os, json

class Database():


    def __init__(self):
        self.tables = []
        self.path = None


    def open(self, path):
        self.path = path
        os.makedirs(self.path, exist_ok = True)
        
        self.bufferpool = BufferPool(pool_size = 32, db_root = path)
        
        tables_dir = os.path.join(self.path, "tables")
        os.makedirs(tables_dir, exist_ok = True)
        
        # load existitng tables from disk
        self.tables = []
        for name in os.listdir(tables_dir):
            table_dir = os.path.join(tables_dir, name)
            meta_path = os.path.join(table_dir, "meta.json")
            
            if os.path.isdir(table_dir) and os.path.exists(meta_path):
                with open(meta_path, 'r') as file:
                    meta = json.load(file)
                
                table = Table(name, meta["num_columns"], meta["key"])
                table.db_root = self.path
                table.bufferpool = self.bufferpool
                table.load(self.path)
                self.tables.append(table)


    def close(self):
        if self.path is None:
            return

        self.bufferpool.flush_all()

        for table in self.tables:
            table.flush(self.path)
            
        for table in self.tables:
            table.mergeQ.put(None)
            if hasattr(table, "_merge_thread"):
                table._merge_thread.join()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # check if table name already exists
        for table in self.tables:
            if table.name == name:
                raise RuntimeError("Table name already exists")
            
        table = Table(name, num_columns, key_index)
        
        table.db_root = self.path
        table.bufferpool = self.bufferpool
        
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                del self.tables[i]
                return
        raise RuntimeError("Table not found")

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        # if table name not found
        return None
