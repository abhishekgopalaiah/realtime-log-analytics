# Remove the entire log_medallion folder
dbutils.fs.rm("/tmp/log_medallion", recurse=True)
