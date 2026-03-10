from lstore.page import Page
import os
import threading

class Frame:

    
    def __init__(self, page):
        self.page = page
        # whether the page has been modified since it was loaded into buffer pool, initially no
        self.dirty = False
        # number of active users using the page, intially none
        self.pin_count = 0
        

    def pin(self):
        self.pin_count += 1
        
        
    def unpin(self):
        if self.pin_count > 0:
            self.pin_count -= 1
    
    
    def mark_dirty(self):
        self.dirty = True
        
        
    def can_evict(self):
        # pin count of 0 means page is not currently being used and can be evicted, otherwise it cannot be evicted
        return self.pin_count == 0
    
    
class BufferPool:
    
    def __init__(self, pool_size, db_root: str):
        self.pool_size = pool_size
        self.db_root = db_root
        
        # mapping of path of page to frame
        self.frames = {}
        # list of paths in order of least recently used to most recently used
        self.lru = []
        
        self.lock = threading.RLock()
        
    
    # page has been accessed, so update its position in lru list by moving it to the end
    def _touch(self, path: str):
        if path in self.lru:
            self.lru.remove(path)
        self.lru.append(path)
        
        
    def _read_page_from_disk(self, path: str):
        # if page doesn't exist on disk, treat as new page and return empty page
        if not os.path.exists(path):
            return Page()
        with open(path, 'rb') as file:
            raw_bytes = file.read()
        return Page.from_bytes(raw_bytes)
    
    
    def _write_page_to_disk(self, path: str, page: Page):
        os.makedirs(os.path.dirname(path), exist_ok = True)
        data = page.to_bytes()
        
        tmp = path + f".{threading.get_ident()}.tmp"
        with open(tmp, "wb") as file:
            file.write(data)
            file.flush()
            
        os.replace(tmp, path)
            
    
    def _evict(self):
        with self.lock:
            # no need to evict if buffer pool is not full
            if len(self.frames) < self.pool_size:
                return
            
            lru_index = 0
            while lru_index < len(self.lru):
                path = self.lru[lru_index]
                frame = self.frames.get(path)
                
                if frame is None:
                    self.lru.pop(lru_index)
                    continue
                
                if frame.can_evict():
                    if frame.dirty:
                        self._write_page_to_disk(path, frame.page)
                        frame.dirty = False
                        
                    self.lru.pop(lru_index)
                    del self.frames[path]
                    return
                
                lru_index += 1
            
            # if no pages are unpinned, then no page can be evicted
            raise RuntimeError("Cannot evict: all pages are in use")
    
    
    def get_page(self, path: str):
        with self.lock:
            # if page is already in buffer pool, update it's position in lru list and return page
            if path in self.frames:
                frame = self.frames[path]
                frame.pin()
                self._touch(path)
                return frame.page
            
            # if buffer pool is full, try to evict
            self._evict()
            
            page = self._read_page_from_disk(path)
            frame = Frame(page)
            frame.pin()
            self.frames[path] = frame
            self._touch(path)
            return page
    
    
    def unpin(self, path: str):
        with self.lock:
            if path in self.frames:
                self.frames[path].unpin()
            
            
    def mark_dirty(self, path: str):
        with self.lock:
            if path in self.frames:
                self.frames[path].mark_dirty()
            
    
    # when database is closed, all dirty pages in buffer pool need to be written back to disk
    def flush_all(self):
        with self.lock:
            for path, frame in list(self.frames.items()):
                if frame.dirty:
                    self._write_page_to_disk(path, frame.page)
                    frame.dirty = False