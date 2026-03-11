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
        self._rolling_back = False


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

        
    def _build_undo_entry(self, query, table, args):
        """
        Build inverse operation BEFORE the query runs.
        Returns:
            None if no undo needed
            tuple(callable, args) for rollback
        """
        query_name = query.__name__
        qobj = query.__self__   # the Query instance bound to this method

        # INSERT -> undo is DELETE(primary_key)
        if query_name == "insert":
            key = args[table.key]
            return (qobj.delete, (key,))

        # UPDATE -> undo is UPDATE(primary_key, old_values_for_changed_columns)
        elif query_name == "update":
            primary_key = args[0]
            new_columns = args[1:]

            old_records = qobj.select(primary_key, table.key, [1] * table.num_columns)
            if not old_records:
                return None

            old_record = old_records[0].columns
            rollback_columns = [None] * table.num_columns

            for i in range(table.num_columns):
                if i == table.key:
                    rollback_columns[i] = None
                elif new_columns[i] is not None:
                    rollback_columns[i] = old_record[i]

            return (qobj.update, (primary_key, *rollback_columns))

        # DELETE -> undo is INSERT(old_full_row)
        elif query_name == "delete":
            primary_key = args[0]

            old_records = qobj.select(primary_key, table.key, [1] * table.num_columns)
            if not old_records:
                return None

            old_record = old_records[0].columns
            return (qobj.insert, tuple(old_record))

        # SELECT / SUM / other reads do not need undo
        return None

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self.undo_log = []
        self._rolling_back = False

        for query, table, args in self.queries:
            undo_entry = self._build_undo_entry(query, table, args)

            result = query(*args)

            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()

            if undo_entry is not None:
                self.undo_log.append(undo_entry)

        return self.commit()

    def abort(self):
        # roll back in reverse order
        self._rolling_back = True

        for undo_query, undo_args in reversed(self.undo_log):
            try:
                undo_query(*undo_args)
            except Exception:
                pass

        self.undo_log = []
        self._rolling_back = False
        return False

    def commit(self):
        self.undo_log = []
        return True
