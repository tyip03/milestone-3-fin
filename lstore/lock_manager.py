import threading

class LockManager:
    
    
    def __init__(self):
        # key = rid, value = transactions holding S or X locks
        self.locks = {}
        self.lock = threading.Lock()
    
    
    def get_s_lock(self, tid, rid):
        with self.lock:
            # create lock entry if doesn't exist
            if rid not in self.locks:
                self.locks[rid] = {"S": set(), "X": None}
                
            lock = self.locks[rid]
            
            # abort if another transaction holds X lock on this rid
            if lock["X"] is not None and lock["X"] != tid:
                return False
            
            lock["S"].add(tid)
            
            return True
        
    
    def get_x_lock(self, tid, rid):
        with self.lock:
            # create lock entry if doesn't exist
            if rid not in self.locks:
                self.locks[rid] = {"S": set(), "X": None}
                
            lock = self.locks[rid]
            
            # abort if another transaction holds X or S lock on this rid
            if lock["X"] is not None and lock["X"] != tid:
                return False
            if lock["S"] and lock["S"] != {tid}:
                return False
            
            lock["S"].discard(tid)
            lock["X"] = tid
            
            return True
    
    
    def release_locks(self, tid):
        with self.lock:
            for rid in list(self.locks.keys()):
                lock = self.locks[rid]
                
                if lock["X"] == tid:
                    lock["X"] = None
                lock["S"].discard(tid)
                
                if not lock["S"] and lock["X"] is None:
                    del self.locks[rid]