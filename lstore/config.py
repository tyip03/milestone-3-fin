# maximum number of records per page
PAGE_SIZE = 4096 
# each record is a 64-bit integer -> 8 bytes
INT_SIZE = 8 

MAX_RECORDS_PER_PAGE = PAGE_SIZE // INT_SIZE

# maximum number of base pages per page range
MAX_BASE_PAGES = 16

# 8 bytes to store the number of records in the page
HEADER_SIZE = 8 

# for base pages, points to latest version of record. for tail pages, points to previous version of record.
INDIRECTION_COLUMN = 0
# unique identifer for each record
RID_COLUMN = 1
# time when record version was created. for base records, time when record was inserted. for tail records, time when record was updated.
TIMESTAMP_COLUMN = 2
# 0 for all base records. for tail records, bitmap for which cols were updated: 1 if updated, 0 if not.
SCHEMA_ENCODING_COLUMN = 3
# tracks RID of original base record for each tail record
BASE_RID_COLUMN = 4