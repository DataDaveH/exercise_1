#!/bin/bash
#
# run all the python scripts for table transformations
# they are assumed to be local
/data/spark15/bin/spark-submit hospitals.py
/data/spark15/bin/spark-submit measures.py
/data/spark15/bin/spark-submit procedures.py
/data/spark15/bin/spark-submit survey_resp.py

exit
