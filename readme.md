#SEN Database Formater/Uploader

Provides functions for downloading, formatting, and pushing to a database information from the SEN (Secretaria Energia de la Nacion) of Argentina.

Main input variables from the user will be to set:

'''shell
STAT_FILE: File where status of run will be stored
PUBLIC_FILE_STORAGE: Directory where to place downloaded Capitulo IV
SQL_PUBLIC_STRING: SQL Alchemy string of server to push informaiton to
FIRST_YEAR: Earliest year to pull data for

'''

From here the script should be able to be run.

It will download all the relevant Capitulo IV's to the indicated **publicFileStorage** it will then format them and push them to the indicated database as table called **CAPITULO_IV**, or if this table already exists, it will update it.

The script will also download the public well headers and create or insert into a table called **LISTADO_DE_POZOS** and public completion data into a table called **DATOS_FRACTURAS**.