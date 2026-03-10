from lstore.table import Table, Record
from lstore.index import Index

NULL = object()

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.key = table.key
        self.index = table.index
        self.columns = table.num_columns

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            rid = rids[0]
            
            # get values for indexed columns before deleting to remove that rid from index
            values = [self.table.read_version(rid, col, 0) for col in range(self.table.num_columns)]
            
            ok = self.table.delete(rid)
            if not ok:
                return False
            
            # remove rid from index for indexed columns
            for col in range(self.table.num_columns):
                if self.table.index.indices[col] is not None:
                    self.table.index.remove_from_index(col, values[col], rid)
            
            return True
        except Exception:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        
        try:
            data_columns = self.table.num_columns 

            if len(columns) != data_columns:
                return False # Invalid number of columns
        
            if any(c is None for c in columns):
                return False # Invalid column value (None), insertion fails
        
            key = columns[self.table.key] # Extract the key value from the columns based on the key index
        
            if self.table.index.locate(self.table.key, key):
                return False # Duplicate key value, insertion fails
        
            self.table.insert(columns)
            return True # Insertion successful
    
        except Exception as e:
            return False # Return False if any exception occurs during the insertion process

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            data_columns = self.table.num_columns
        
            def get_record_by_rid(rid):
                record_data = [None] * data_columns
                for i in range(data_columns):
                    if projected_columns_index[i] == 1: # Check if the column is projected
                        record_data[i] = self.table.read_version(rid, i, 0) # Read the value from the table for the projected column
                    
                key_value = self.table.read_version(rid, self.table.key, 0) # Get the key value for the record
                return Record(rid, key_value, record_data)
        
            if self.table.index.indices[search_key_index] is not None:
                rids = self.table.index.locate(search_key_index, search_key)
            else:
                rids = []
                for rid in self.table.page_directory.keys():
                    key_val = self.table.read_version(rid, search_key_index, 0)
                    if key_val in (0, None):
                        continue
                    if key_val == search_key:
                        rids.append(rid)
            
            records = []
            for rid in rids:
                records.append(get_record_by_rid(rid))
            return records
        
        except Exception:
            return []

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            def get_record_by_rid(rid):
                record_data = [None] * self.table.num_columns
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        record_data[i] = self.table.read_version(rid, i, relative_version)

                key_value = self.table.read_version(rid, self.table.key, relative_version)
                return Record(rid, key_value, record_data)

            if self.table.index.indices[search_key_index] is not None:
                rids = self.table.index.locate(search_key_index, search_key)
            else:
                rids = []
                for rid in self.table.page_directory.keys():
                    key_val = self.table.read_version(rid, search_key_index, relative_version)
                    if key_val in (0, None):
                        continue
                    if key_val == search_key:
                        rids.append(rid)
            
            records = []
            for rid in rids:
                records.append(get_record_by_rid(rid))
            return records

        except Exception:
            return []

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False
            
            # key column value must be None because it cannot be updated
            if list(columns)[self.table.key] is not None:
                return False

            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            rid = rids[0]
            return self.table.update(rid, *columns)
        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False # No records found in the given range, return False
            
            total_sum = 0
            found = False
            for rid in rids:
                # for each rid, read its value from the column to be aggregated
                val = self.table.read_version(rid, aggregate_column_index, 0)
                # abort summation if there's a record without a value within the range
                if val is None:
                    continue
                total_sum += val
                found = True
            
            if not found:
                return False
            return total_sum 
        
        except Exception:
            return False
            
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # get list of RIDs within the specified range
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        # if rids is empty then there are no records within the range
        if not rids:
            return False
        
        total_sum = 0
        for rid in rids:
            # for each specific version of the record, read its value from the column to be aggregated
            val = self.table.read_version(rid, aggregate_column_index, relative_version)
            # abort summation if there's a record without a value within the range
            if val is None:
                return False
            total_sum += val
        
        return total_sum

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        try:
            data_columns = self.table.num_columns 
    
            r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
            if r is not False:
                updated_columns = [None] * self.table.num_columns
                updated_columns[column] = r[column] + 1
                u = self.update(key, *updated_columns)
                return u
            
            return False
        except Exception as e:
            return False # Return False if any exception occurs during the increment process
