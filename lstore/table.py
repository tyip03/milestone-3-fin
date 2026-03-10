from lstore.index import Index
from time import time
from lstore.page import Page
from lstore.config import INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, MAX_BASE_PAGES, BASE_RID_COLUMN
import os, json
import threading
import queue
import struct
from lstore.lock_manager import LockManager

class PageRange:
    
    
    """
    :param max_base_pages: int     #maximum number of base pages allowed
    :param num_col: int            #number of columns in table
    """
    def __init__(self, num_col, max_base_pages):
        self.num_col = num_col
        self.max_base_pages = max_base_pages
        
        self.bufferpool = None
        self.db_root = None
        
        # each column gets a list of pages
        self.base_pages = [[] for _ in range(num_col)]
        self.tail_pages = [[] for _ in range(num_col)]
        
        # initalize first tail page id to 0 for each column
        for col in range(num_col):
            self.tail_pages[col].append(0)
        
        
    # check if base page range has capacity
    def base_has_capacity(self):
        # if any column is full, then the page range is full
        return len(self.base_pages[0]) < self.max_base_pages
    
    
    def add_base_page(self):
        if not self.base_has_capacity():
            raise RuntimeError("Base page range is full")
        
        # add base page id for each column
        for col in range(len(self.base_pages)):
            new_page_id = len(self.base_pages[col])
            self.base_pages[col].append(new_page_id)
        
        
    def add_tail_page(self):
        # add tail page id for each column
        for col in range(len(self.tail_pages)):
            new_page_id = len(self.tail_pages[col])
            self.tail_pages[col].append(new_page_id)
            
        
class Record:
    
    
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        

class Table:


    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.tail_page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 10  # The threshold to trigger a merge
        self.page_ranges = []
        self.rid_counter = 0
        self.mergeQ = queue.Queue()          # queue of page_range_ids to merge
        self.deallocateQ = queue.Queue()
        self._merge_scheduled = set()
        
        self._merge_thread = threading.Thread(target = self._merge_worker, daemon = True)
        self._merge_thread.start()
        
        self.lock_manager = LockManager()
        
        self.lock = threading.RLock()
        
    """
    :param record: list[int]     #list of column values to be inserted
    """     
    def insert(self, record):
        with self.lock:
            rid = self.rid_counter
            # increment rid_counter to ensure RID uniqueness for every insert
            self.rid_counter += 1
            
            indirection = 0
            timestamp = int(time())
            schema_encoding = 0
            base_rid = rid
            
            user_record = list(record)
            record = [indirection, rid, timestamp, schema_encoding, base_rid] + user_record
            
            # create page range if there isn't one or if last page range is full
            if not self.page_ranges or not self.page_ranges[-1].base_has_capacity():
                self.page_ranges.append(PageRange(self.num_columns + 5, MAX_BASE_PAGES))
            
            last_page_range = self.page_ranges[-1]
            page_range_ind = len(self.page_ranges) - 1
            
            # check capacity after getting page from bufferpool
            if not last_page_range.base_pages[0]:
                last_page_range.add_base_page()
                
            # check if last base page is full
            page_id0 = last_page_range.base_pages[0][-1]
            path0 = self._page_path("base", page_range_ind, 0, page_id0)
            page0 = self.bufferpool.get_page(path0)
            
            if not page0.has_capacity():
                self.bufferpool.unpin(path0)
                last_page_range.add_base_page()
                
                page_id0 = last_page_range.base_pages[0][-1]
                path0 = self._page_path("base", page_range_ind, 0, page_id0)
                page0 = self.bufferpool.get_page(path0)
            
            # done checking if there's space
            self.bufferpool.unpin(path0)
            
            # write each value into its column's last base page using bufferpool
            offset = None
            for col, val in enumerate(record):
                page_id = last_page_range.base_pages[col][-1]
                path = self._page_path("base", page_range_ind, col, page_id)
                page = self.bufferpool.get_page(path)
                
                page.write(val)
                self.bufferpool.mark_dirty(path)
                
                if col == 0:
                    offset = page.num_records - 1
                    
                self.bufferpool.unpin(path)
                
            # update page directory
            page_ind = len(last_page_range.base_pages[0]) - 1
            self.page_directory[rid] = (page_range_ind, page_ind, offset)
            
            # add rid to every index for the record's column values
            for col in range(self.num_columns):
                if self.index.indices[col] is not None:
                    self.index.add_to_index(col, user_record[col], rid)
            
            return rid
    
    
    """
    :param rid: int
    :param col: int     #user column index
    """ 
    def read(self, rid, col):
        with self.lock:
            # get record location
            page_range_ind, page_ind, offset = self.page_directory[rid]
            
            page_range = self.page_ranges[page_range_ind]
            
            # get base page data from buffer pool
            base_page_id = page_range.base_pages[col + 5][page_ind]
            base_path = self._page_path("base", page_range_ind, col + 5, base_page_id)
            base_page = self.bufferpool.get_page(base_path)
            
            # get base indirection page from buffer pool
            indir_page_id = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
            indir_path = self._page_path("base", page_range_ind, INDIRECTION_COLUMN, indir_page_id)
            indir_page = self.bufferpool.get_page(indir_path)
            tail_rid = indir_page.read(offset)
            
            # no tail record, read from base page
            if tail_rid in [0, None]:
                val = base_page.read(offset)
                
                # unpin pinned pages
                self.bufferpool.unpin(indir_path)
                self.bufferpool.unpin(base_path)
                
                return val
        
            # tail record exists, get record location from tail page directory 
            tail_page_range_ind, tail_page_ind, tail_offset = self.tail_page_directory[tail_rid]
            
            # get tail page from buffer pool
            tail_path = self._page_path("tail", tail_page_range_ind, col + 5, tail_page_ind)
            tail_page = self.bufferpool.get_page(tail_path)
            
            # get tail schema page from buffer pool
            schema_path = self._page_path("tail", tail_page_range_ind, SCHEMA_ENCODING_COLUMN, tail_page_ind)
            schema_page = self.bufferpool.get_page(schema_path)
            schema_encoding = schema_page.read(tail_offset)
            
            # get bit position for column in schema encoding bitmap
            bit_ind = col 
            
            # read from tail page if column was updated (bit value 1), else read from base page
            if (schema_encoding >> bit_ind) & 1:
                val = tail_page.read(tail_offset)
            else:
                val = base_page.read(offset)
                
            # unpin pinned pages
            self.bufferpool.unpin(schema_path)
            self.bufferpool.unpin(tail_path)
            self.bufferpool.unpin(indir_path)
            self.bufferpool.unpin(base_path)
            
            return val
        
    """
    :param rid: int
    :param col: int                     #user column index
    :param relative_version: int        #relative version of record to be read
    """     
    def read_version(self, rid, col, relative_version):
        with self.lock:
            # get record location
            page_range_ind, page_ind, offset = self.page_directory[rid]
            page_range = self.page_ranges[page_range_ind]
            
            # get base indirection page from buffer pool
            base_indir_page_id = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
            base_indir_path = self._page_path("base", page_range_ind, INDIRECTION_COLUMN, base_indir_page_id)
            base_indir_page = self.bufferpool.get_page(base_indir_path)
            tail_rid = base_indir_page.read(offset)
            
            # walk through the tail chain to look for relative version
            current_version = 0
            while tail_rid not in [0, None]:
                # find record location from tail page directory
                tail_page_range_ind, tail_page_ind, tail_offset = self.tail_page_directory[tail_rid]
                tail_page_range = self.page_ranges[tail_page_range_ind]
                    
                # get tail schema page from buffer pool
                schema_page_id = tail_page_range.tail_pages[SCHEMA_ENCODING_COLUMN][tail_page_ind]
                schema_path = self._page_path("tail", tail_page_range_ind, SCHEMA_ENCODING_COLUMN, schema_page_id)
                schema_page = self.bufferpool.get_page(schema_path)
                schema = schema_page.read(tail_offset)
                
                # if bit value is 1, column was updated
                if ((schema >> col) & 1) == 1:
                    if current_version == relative_version:
                        
                        data_page_id = tail_page_range.tail_pages[col + 5][tail_page_ind]
                        data_path = self._page_path("tail", tail_page_range_ind, col + 5, data_page_id)
                        data_page = self.bufferpool.get_page(data_path)
                        val = data_page.read(tail_offset)
                        
                        self.bufferpool.unpin(data_path)
                        self.bufferpool.unpin(schema_path)
                        self.bufferpool.unpin(base_indir_path)
                        
                        return val
                    
                    current_version -= 1
                    
                # if not then check previous tail record
                indir_page_id = tail_page_range.tail_pages[INDIRECTION_COLUMN][tail_page_ind]
                indir_path = self._page_path("tail", tail_page_range_ind, INDIRECTION_COLUMN, indir_page_id)
                indir_page = self.bufferpool.get_page(indir_path)
                prev_tail_rid = indir_page.read(tail_offset)
                tail_rid = prev_tail_rid
                    
                self.bufferpool.unpin(indir_path)
                self.bufferpool.unpin(schema_path)
            
            # if didn't unpin earlier
            self.bufferpool.unpin(base_indir_path)      
            
            # if no update for column was found after going through all tail records, read from base page    
            base_data_page_id = page_range.base_pages[col + 5][page_ind]   
            base_data_path = self._page_path("base", page_range_ind, col + 5, base_data_page_id)    
            base_page = self.bufferpool.get_page(base_data_path)
            val = base_page.read(offset)
            
            self.bufferpool.unpin(base_data_path)
            
            return val
    
        
    """
    :param rid: int
    :param *cols: tuple     #updated column values
    """  
    def update(self, rid, *cols):
        with self.lock:
            # get record location
            page_range_ind, page_ind, offset = self.page_directory[rid]
            page_range = self.page_ranges[page_range_ind]
            
            # get base indirection from buffer pool
            base_indir_page_id = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
            base_indir_path = self._page_path("base", page_range_ind, INDIRECTION_COLUMN, base_indir_page_id)
            base_indir_page = self.bufferpool.get_page(base_indir_path)
            tail_rid = base_indir_page.read(offset)
            
            new_tail_rid = self.rid_counter
            self.rid_counter += 1
            
            timestamp = int(time())
            indirection = tail_rid
            schema_encoding = 0
            base_rid = rid
            tail_record = [indirection, new_tail_rid, timestamp, schema_encoding, base_rid]
            
            # update schema encoding bitmap (1 for updated, 0 for not updated)
            for i , val in enumerate(cols):
                if val is not None:
                    schema_encoding |= 1 << i
                    tail_record.append(val)
                else:
                    tail_record.append(0)
            
            tail_record[SCHEMA_ENCODING_COLUMN] = schema_encoding
            
            # make sure tail page exists for all cols being written to
            if not page_range.tail_pages[0]:
                for col_id in range(len(tail_record)):
                    page_range.add_tail_page()
            
            # check capacity using 0 column
            page_id0 = page_range.tail_pages[0][-1]
            path0 = self._page_path("tail", page_range_ind, 0, page_id0)
            page0 = self.bufferpool.get_page(path0)
            
            if not page0.has_capacity():
                self.bufferpool.unpin(path0)
                page_range.add_tail_page()  # allocate a new tail page for every column
                    
                page_id0 = page_range.tail_pages[0][-1]
                path0 = self._page_path("tail", page_range_ind, 0, page_id0)
                page0 = self.bufferpool.get_page(path0)

                page_id0 = page_range.tail_pages[0][-1]
                path0 = self._page_path("tail", page_range_ind, 0, page_id0)
                page0 = self.bufferpool.get_page(path0)
                
            tail_offset = page0.num_records
            self.bufferpool.unpin(path0)
            
            tail_page_ind = len(page_range.tail_pages[0]) - 1
            
            # update indices for updated columns
            for col, new_val in enumerate(cols):
                if new_val is not None and self.index.indices[col] is not None:
                    # old value is the latest version before update
                    old_val = self.read_version(rid, col, 0)
                    # remove rid from old value index
                    self.index.remove_from_index(col, old_val, rid)
                    # add rid to new value index
                    self.index.add_to_index(col, new_val, rid)
            
            # write tail record to tail page using bufferpool
            for col_id, val in enumerate(tail_record):
                page_id = page_range.tail_pages[col_id][-1]
                path = self._page_path("tail", page_range_ind, col_id, page_id)
                page = self.bufferpool.get_page(path)
                
                page.write(val)
                self.bufferpool.mark_dirty(path)
                self.bufferpool.unpin(path)
            
            # update indirection column in base record to point to new tail record
            base_indir_page.update(offset, new_tail_rid)
            self.bufferpool.mark_dirty(base_indir_path)
            self.bufferpool.unpin(base_indir_path)
                
            # update tail page directory
            self.tail_page_directory[new_tail_rid] = (page_range_ind, tail_page_ind, tail_offset)

            # schedule merge if tail is getting large
            if len(page_range.tail_pages[0]) >= self.merge_threshold_pages:
                if page_range_ind not in self._merge_scheduled:
                    self._merge_scheduled.add(page_range_ind)
                    self.mergeQ.put(page_range_ind)
            
            return True
    
    
    def _table_dir(self, db_root: str):
        return os.path.join(db_root, "tables", self.name)
    
    
    def _page_dir(self, db_root: str, page_type: str, range_id: int):
        return os.path.join(self._table_dir(db_root), page_type, f"page_range_{range_id}")
    
    
    def _page_path(self, page_type: str, range_id: int, col_id: int, page_id: int):
        return os.path.join(
            self.db_root,
            self.name,
            page_type,
            f"range_{range_id}",
            f"col_{col_id}_page_{page_id}.bin"
        )
        
        
    def _rebuild_index(self):
        self.index = Index(self)   
        
        # create indices for every col
        for col in range(self.num_columns):
            if self.index.indices[col] is None:
                   self.index.indices[col] = {'index': {}}
                   
        # add every record back into indexed columns
        for rid in self.page_directory.keys():
            for col in range(self.num_columns):
                val = self.read_version(rid, col, 0)
                self.index.add_to_index(col, val, rid)
    
    
    def _rebuild_page_directory(self):
        self.page_directory = {}
        
        for page_range_ind, page_range in enumerate(self.page_ranges):
            if not page_range.base_pages or not page_range.base_pages[RID_COLUMN]:
                continue
            
            for page_ind, rid_page in enumerate(page_range.base_pages[RID_COLUMN]):
                rid_path = self._page_path("base", page_range_ind, RID_COLUMN, rid_page)
                rid_page = self.bufferpool.get_page(rid_path)
                
                for offset in range(rid_page.num_records):
                    rid = rid_page.read(offset)
                    self.page_directory[rid] = (page_range_ind, page_ind, offset)
                    
                self.bufferpool.unpin(rid_path)
    
    
    def _rebuild_tail_page_directory(self):
        self.tail_page_directory = {}
        
        for page_range_ind, page_range in enumerate(self.page_ranges):
            if not page_range.tail_pages or not page_range.tail_pages[RID_COLUMN]:
                continue
            
            for page_ind, rid_page in enumerate(page_range.tail_pages[RID_COLUMN]):
                rid_path = self._page_path("tail", page_range_ind, RID_COLUMN, rid_page)
                rid_page = self.bufferpool.get_page(rid_path)
                
                for offset in range(rid_page.num_records):
                    tail_rid = rid_page.read(offset)
                    #if tail_rid is None:
                    if tail_rid in (0, None):
                        continue
                    self.tail_page_directory[tail_rid] = (page_range_ind, page_ind, offset)
                    
                self.bufferpool.unpin(rid_path)
    
    
    def flush(self, db_root: str):
        table_dir = self._table_dir(db_root)
        os.makedirs(table_dir, exist_ok = True)
        
        # write metadata
        meta = {
            "name": self.name,
            "num_columns": self.num_columns,
            "key": self.key,
            "rid_counter": self.rid_counter,
            "num_page_ranges": len(self.page_ranges),
            "base_pages": [page_range.base_pages for page_range in self.page_ranges],
            "tail_pages": [page_range.tail_pages for page_range in self.page_ranges],
            
            # open() needs to know how many files to load
            "base_page_counts": [len(page_range.base_pages[0]) if page_range.base_pages and page_range.base_pages[0] else 0
                                 for page_range in self.page_ranges],
            "tail_page_counts": [
                [len(page_range.tail_pages[col]) for col in range(len(page_range.tail_pages))]
                for page_range in self.page_ranges
            ]
        }
        
        with open(os.path.join(table_dir, "meta.json"), "w") as file:
            json.dump(meta, file)
            
    
    def load(self, db_root):
        table_dir = os.path.join(db_root, "tables", self.name)
        
        # read metadata
        with open(os.path.join(table_dir, "meta.json"), "r") as file:
            meta = json.load(file)
            
        self.num_columns = meta["num_columns"]
        self.key = meta["key"]
        self.rid_counter = meta["rid_counter"]
        
        num_page_ranges = meta["num_page_ranges"]
        
        # rebuild page ranges
        self.page_ranges = []
        for range_id in range(num_page_ranges):
            page_range = PageRange(self.num_columns + 5, MAX_BASE_PAGES)
            page_range.base_pages = meta["base_pages"][range_id]
            page_range.tail_pages = meta["tail_pages"][range_id]
            self.page_ranges.append(page_range)
            
            """
            # rebuild pages as page ids
            base_count = meta["base_page_counts"][range_id]
            for col_id in range(len(page_range.base_pages)):
                page_range.base_pages[col_id] = list(range(base_count))
            
            # rebuild tail pages as page ids
            tail_counts = meta["tail_page_counts"][range_id]
            for col_id in range(len(page_range.tail_pages)):
                page_range.tail_pages[col_id] = list(range(tail_counts[col_id]))"""
            
        self._rebuild_tail_page_directory()
        self._rebuild_page_directory()
        self._rebuild_index()
        
        
    def delete(self, rid):
        with self.lock:
            if rid not in self.page_directory:
                return False
            
            page_range_ind, page_ind, offset = self.page_directory[rid]
            page_range = self.page_ranges[page_range_ind]
            
            try:
                rid_page_id = page_range.base_pages[RID_COLUMN][page_ind]
                rid_path = self._page_path("base", page_range_ind, RID_COLUMN, rid_page_id)
                rid_page = self.bufferpool.get_page(rid_path)
                rid_page.update(offset, 0)
                self.bufferpool.mark_dirty(rid_path)
                self.bufferpool.unpin(rid_path)
                
                
                indir_page_id = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
                indir_path = self._page_path("base", page_range_ind, INDIRECTION_COLUMN, indir_page_id)
                indir_page = self.bufferpool.get_page(indir_path)
                indir_page.update(offset, 0)
                self.bufferpool.mark_dirty(indir_path)
                self.bufferpool.unpin(indir_path)
                
                del self.page_directory[rid]
                
                return True
            except Exception:
                return False


    def __read_page_direct(self, path):
        if not os.path.exists(path):
            return Page()
        with open(path, "rb") as f:
            raw = f.read()
        return Page.from_bytes(raw)


    def __write_page_direct(self, path, page):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = page.to_bytes()
        
        tmp = path + f".{threading.get_ident()}.tmp"
        
        with open(tmp, "wb") as file:
            file.write(data)
            file.flush()
            
        os.replace(tmp, path)
        
    
    def _read_latest_page(self, path):
        p = self.__read_page_direct(path)
        raw_bytes = p.to_bytes()
        self.bufferpool.unpin(path)
        return Page.from_bytes(raw_bytes)
            
            
    def _merge_worker(self):
        while True:
            range_id = self.mergeQ.get()
            if range_id is None:
                break
            try:
                self.__merge_page_range(range_id)
            finally:
                self._merge_scheduled.discard(range_id)
                
        with self.lock:
            self._merge_scheduled.discard(range_id)


    def __merge_page_range(self, range_id):
        with self.lock:
            if range_id < 0 or range_id >= len(self.page_ranges):
                return False

            page_range = self.page_ranges[range_id]
            total_cols = self.num_columns + 5

            base_page_count = len(page_range.base_pages[0]) if page_range.base_pages and page_range.base_pages[0] else 0
            if base_page_count == 0:
                return True

            old_base_ids = [[page_range.base_pages[c][i] for i in range(base_page_count)] for c in range(total_cols)]

            cons_pages = {} 
            new_base_ids = [[] for _ in range(total_cols)]

            for col in range(total_cols):
                for page_ind in range(base_page_count):
                    old_id = page_range.base_pages[col][page_ind]
                    old_path = self._page_path("base", range_id, col, old_id)
                    base_copy = self._read_latest_page(old_path)

                    new_id = len(page_range.base_pages[col]) + page_ind
                    new_base_ids[col].append(new_id)
                    cons_pages[(col, page_ind)] = base_copy

            base_rids = []
            rid_page_ids = page_range.base_pages[RID_COLUMN]
            
            for base_page_ind, rid_page_id in enumerate(rid_page_ids):
                rid_path = self._page_path("base", range_id, RID_COLUMN, rid_page_id)
                rid_page = self._read_latest_page(rid_path)
                for base_offset in range(rid_page.num_records):
                    base_rid = rid_page.read(base_offset)
                    if base_rid in (0, None):
                        continue
                    base_rids.append((base_rid, base_page_ind, base_offset))
                    
            base_lookup = {
                rid: (page_ind, offset)
                for rid, page_ind, offset in base_rids
            }
            
            seenUpdates = set()

            tail_rid_page_ids = page_range.tail_pages[RID_COLUMN]
            for tail_page_ind in range(len(tail_rid_page_ids) - 1, -1, -1):
                rid_page_id = tail_rid_page_ids[tail_page_ind]
                rid_path = self._page_path("tail", range_id, RID_COLUMN, rid_page_id)
                rid_page = self._read_latest_page(rid_path)
                
                base_rid_page_id = page_range.tail_pages[BASE_RID_COLUMN][tail_page_ind]
                base_rid_path = self._page_path("tail", range_id, BASE_RID_COLUMN, base_rid_page_id)
                base_rid_page = self._read_latest_page(base_rid_path)

                for tail_offset in range(rid_page.num_records - 1, -1, -1):
                    
                    if tail_offset >= base_rid_page.num_records:
                        continue
                    
                    base_rid = base_rid_page.read(tail_offset)
                    
                    
                    base_info = base_lookup.get(base_rid)
                    if not base_info:
                        continue
                    
                    base_page_ind, base_offset = base_info

                    schema_page_id = page_range.tail_pages[SCHEMA_ENCODING_COLUMN][tail_page_ind]
                    schema_path = self._page_path("tail", range_id, SCHEMA_ENCODING_COLUMN, schema_page_id)
                    schema_page = self._read_latest_page(schema_path)
                    schema = schema_page.read(tail_offset)

                    for user_col in range(self.num_columns):
                        if ((schema >> user_col) & 1) == 0:
                            continue

                        tail_col_page_id = page_range.tail_pages[user_col + 5][tail_page_ind]
                        tail_col_path = self._page_path("tail", range_id, user_col + 5, tail_col_page_id)
                        tail_col_page = self._read_latest_page(tail_col_path)
                        
                        if tail_offset >= tail_col_page.num_records:
                            continue
                        
                        new_val = tail_col_page.read(tail_offset)

                        cons_page = cons_pages[(user_col + 5, base_page_ind)]
                        cons_page.update(base_offset, new_val)

                    seenUpdates.add(base_rid)

                    if len(seenUpdates) == len(base_rids):
                        break

                if len(seenUpdates) == len(base_rids):
                    break

            for col in range(total_cols):
                for page_ind in range(base_page_count):
                    new_id = new_base_ids[col][page_ind]
                    new_path = self._page_path("base", range_id, col, new_id)
                    
                    if col == INDIRECTION_COLUMN:
                        old_id = page_range.base_pages[col][page_ind]
                        old_path = self._page_path("base", range_id, col, old_id)
                        latest_indir = self._read_latest_page(old_path)
                        self.__write_page_direct(new_path, latest_indir)
                    else:
                        self.__write_page_direct(new_path, cons_pages[col, page_ind])

            for col in range(total_cols):
                for page_ind in range(base_page_count):
                    page_range.base_pages[col][page_ind] = new_base_ids[col][page_ind]

            self.deallocateQ.put((range_id, old_base_ids))

            return True
