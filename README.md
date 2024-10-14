# jrc_common
Library of common utility functions for Python

## Installation
pip install git+https://github.com/JaneliaSciComp/jrc_common.git

The following key must be present in the run environment:

    CONFIG_SERVER_URL: base URL for Configuration system (CONFIG_SERVER_URL=https://config.int.janelia.org/)


Note that database libraries are not automatically installed. To use the
database connection routines, install:

    - mysqlclient==2.1.1
    - psycopg2
    - psycopg2.extras
    - pymongo==4.4.0
