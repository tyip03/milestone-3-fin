import threading

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns
        # needed 'index' key because it is used in other methods
        self.indices[table.key] = {'index': {}}  # Initialize the key column index as a dictionary for O(1) lookups
        
        self.lock = threading.RLock()
        

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        with self.lock:
            # return empty list if column number is invalid
            if column < 0 or column >= self.table.num_columns:
                return []
            
            # return empty list if column is not indexed
            if self.indices[column] is None:
                return []

            # get hash index dictionary for column
            bucket = self.indices[column]['index']
            # if exists, return value associated with key value in dictionary
            return bucket.get(value, [])


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        with self.lock:
            if column < 0 or column >= self.table.num_columns:
                return []
            
            if self.indices[column] is None:
                return []
            
            rids = []
            # get hash index dictionary for column
            bucket = self.indices[column]['index']
            for key, key_rids in bucket.items():
                # check that key is within specified range
                if begin <= key <= end:
                    rids.extend(key_rids)
            return rids
     
    
    def create_index(self, column):
        if column < 0 or column >= self.table.num_columns:
            return
        
        with self.lock:
            if self.indices[column] is not None:
                return
            
        entries = []
        rids = list(self.table.page_directory.keys())
        
        for rid in rids:
            value = self.table.read_version(rid, column, 0)
            entries.append((rid, value))
            
        with self.lock:
            if self.indices[column] is not None:
                return
            
            self.indices[column] = {'index': {}}
            
            for rid, value in entries:
                if value not in self.indices[column]['index']:
                    self.indices[column]['index'][value] = []
                if rid not in self.indices[column]['index'][value]:
                    self.indices[column]['index'][value].append(rid)

    
    # optional: Drop index of specific column
    def drop_index(self, column_number):
        with self.lock:
            self.indices[column_number] = None


    def add_to_index(self, column, value, rid):
        with self.lock:
            if column < 0 or column >= self.table.num_columns:
                return
            
            if self.indices[column] is None:
                return
            
            bucket = self.indices[column]['index']
            # if key value is not in index, create an empty list for its RID
            if value not in bucket:
                bucket[value] = []
            # makes sure no RID is added more than once for the smame key value
            if rid not in bucket[value]:
                bucket[value].append(rid)


    def remove_from_index(self, column, value, rid):
        with self.lock:
            if column < 0 or column >= self.table.num_columns:
                return
            
            # if index doesn't exist
            if self.indices[column] is None:
                return
            
            bucket = self.indices[column]['index']
            # if value is not in index
            if value not in bucket:
                return
            
            # if RID in index, remove
            if rid in bucket[value]:
                bucket[value].remove(rid)
                # if value doesn't have any RIDs, delete value from index
                if len(bucket[value]) == 0:
                    del bucket[value]