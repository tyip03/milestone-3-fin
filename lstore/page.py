import struct
from lstore.config import INT_SIZE, PAGE_SIZE, HEADER_SIZE, MAX_RECORDS_PER_PAGE

class Page:
    

    def __init__(self):
        self.num_records = 0
        # allocate 4096 bytes of empty space
        self.data = bytearray(PAGE_SIZE)
        

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE
    
    """
    :param value: int     
    """ 
    def write(self, value):
        if not self.has_capacity():
            raise RuntimeError("Page is full")
        
        # compute the start index to insert new record at
        start = self.num_records * INT_SIZE
        # insert the integer as 8 bytes with order of bytes being least significant first
        self.data[start:start + INT_SIZE] = value.to_bytes(INT_SIZE, byteorder = 'little')
        
        self.num_records += 1
        
    """
    :param value: int
    """     
    def read(self, value):
        if value < 0 or value >= self.num_records:
            raise RuntimeError("Index out of bounds")
        
        # compute the start index to read record from
        start = value * INT_SIZE
        # read 8 bytes from bytearray as one integer with order of bytes matching how it was written
        return int.from_bytes(self.data[start:start + INT_SIZE], byteorder='little')
    

    # might not need for milestone 1
    def update(self, offset, value):
        if offset <0 or offset >= self.num_records:
            raise RuntimeError("Index out of bounds")

        start = offset * INT_SIZE
        self.data[start:start + INT_SIZE] = int(value).to_bytes(INT_SIZE, byteorder='little', signed=True)
        
    # turn the page into raw bytes to be written to disk
    def to_bytes(self):
        # store the number of records in page as first 8 bytes of page 
        header = struct.pack('<Q', self.num_records)
        
        if len(self.data) != PAGE_SIZE:
            raise RuntimeError("Page must be exactly PAGE_SIZE bytes")
        
        return header + bytes(self.data)
    
    
    # turn raw_bytes from disk into page object
    @classmethod
    def from_bytes(cls, raw_bytes):
        # make sure size is correct
        if len(raw_bytes) != HEADER_SIZE + PAGE_SIZE:
            raise RuntimeError("Raw bytes must be exactly HEADER + PAGE_SIZE bytes")
        
        # unpack first 8 bytes to get num_records
        (num_records,) = struct.unpack('<Q', raw_bytes[:HEADER_SIZE])
        
        # extract page data
        data = raw_bytes[HEADER_SIZE:]
        
        # create page object 
        page = cls()
        page.num_records = num_records
        page.data[:] = data
        
        return page