from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.undo_log = []
        self.tid = id(self)


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            query_name = query.__name__
            primary_key = args[0] if len(args) > 0 else None
            table_query = Query(table)
            
            lock_id = primary_key
            if query_name in ["select", "update", "delete"]:
                rids = table.index.locate(table.key, primary_key)
                if not rids:
                    return self.abort()
                lock_id = rids[0]
            
            if query_name == "select":
                if not table.lock_manager.get_s_lock(self.tid, primary_key):
                    return self.abort()
            elif query_name in ["update", "delete", "insert"]:
                if not table.lock_manager.get_x_lock(self.tid, primary_key):
                    return self.abort()
                
            if query_name == "update":
                old_record = table_query.select(primary_key, table.key, [1] * table.num_columns)
                if old_record == False or len(old_record) == 0:
                    return self.abort()
                
                old_vals = old_record[0].columns.copy()
                self.undo_log.append(("update", table, primary_key, old_vals))
                
            elif query_name == "delete":
                old_record = table_query.select(primary_key, table.key, [1] * table.num_columns)
                if old_record == False or len(old_record) == 0:
                    return self.abort()
                
                old_vals = old_record[0].columns.copy()
                self.undo_log.append(("delete", table, old_vals))
                
            elif query_name == "insert":
                existing = table_query.select(primary_key, table.key, [1] * table.num_columns)
                if existing and len(existing) > 0:
                    return self.abort()
                self.undo_log.append(("insert", table, primary_key))
                
            result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        
        for entry in reversed(self.undo_log):
            action = entry[0]
            
            if action == "update":
                _, table, primary_key, old_vals = entry
                
                restored_cols = old_vals.copy()
                restored_cols[table.key] = None
                table_query = Query(table)
                table_query.update(primary_key, *restored_cols)
            
            elif action == "delete":
                _, table, old_vals = entry
                
                table_query = Query(table)
                table_query.insert(*old_vals)
            
            elif action == "insert":
                _, table, primary_key = entry
                
                table_query = Query(table)
                table_query.delete(primary_key)

        released = set()
        for _, table, _ in self.queries:
            if id(table) not in released:
                table.lock_manager.release_locks(self.tid)
                released.add(id(table))
        
        self.undo_log.clear()
        return False

    
    def commit(self):
        # TODO: commit to database
        
        released = set()
        for _, table, _ in self.queries:
            if id(table) not in released:
                table.lock_manager.release_locks(self.tid)
                released.add(id(table))
        
        self.undo_log.clear()
        
        return True
