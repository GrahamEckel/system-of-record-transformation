# System of Record Transformation (Sort)
SoRt handles the prioritizing, sorting, merging, and deduplication of columns, rows and individual cells of data based on business-defined logic. It will accept many tables from many data sources and output single cleaned tables as per a business-defined structure. 

It has been tested on Static and Master data with minimal performance issues. Some performance slow-downs when tested on high dimensional Transactional data. Consider SoRT a custom 'small data' BI/ETL solution for roughly 100 input sources, each with rows less than 20 million and columns less than 30.  
