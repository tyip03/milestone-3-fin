from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = None

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread is not None:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # Spec says aborted transactions should keep retrying until commit
            while True:
                committed = transaction.run()
                if committed:
                    self.stats.append(True)
                    break

        self.result = len(list(filter(lambda x: x, self.stats)))
