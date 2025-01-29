"""
Standard Landtable Core error messages.
"""

UNAVAILABLE_ETCD = "Landtable cannot communicate with etcd, try again later"
WORKSPACE_NOT_FOUND = "workspace {workspace} does not exist or you do not have permission to access it"
DATABASE_NOT_FOUND = "database {database} does not exist or you do not have permission to access it"
TABLE_NOT_FOUND = "table {table} does not exist or you do not have permission to access it"
WRONG_NAMESPACE = "expected namespace {namespace} for identifier {identifier}"