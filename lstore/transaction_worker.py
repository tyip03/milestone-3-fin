from lstore.table import Table, Record
from lstore.index import Index
import threading
import time

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = [] if transactions is None else list(transactions)
        self.result = 0

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.thread = threading.Thread(target = self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()      
        
        
    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            result = False
            while not result:
                result = transaction.run()
                if not result:
                    time.sleep(0.001)
            self.stats.append(result)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
