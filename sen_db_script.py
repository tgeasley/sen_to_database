"""
Provides functions for downloading, formatting, and pushing to a database
information from the SEN of Argentina.

Main input variables from the user will be to set:

STAT_FILE: File where status of run will be stored
PUBLIC_FILE_STORAGE: Directory where to place downloaded Capitulo IV
SQL_PUBLIC_STRING: SQL Alchemy string of server to push informaiton to

"""




#Imports
import sqlalchemy as sa
from sqlalchemy.orm import Session, Query
from sqlalchemy  import text, MetaData, Table, Column, ForeignKey
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import null
import pandas as pd
import numpy as np
import urllib
import time
from lxml import html
import requests
import datetime as dt
from pathlib import Path
import sys


#Versioning info
__author__ = "Greg Easley"
__copyright__ = "Copyright 2020, easley.dev"
__credits__ = ["Greg Easley"]
__license__ = "GPL"
__version__ = "0.1"
__maintainer__ = "Greg Easley"
__email__ = "greg@easley.dev"
__status__ = "Beta"


################################
##### SAVE FILE INFO ###########
################################


STAT_FILE = open('./database_status.txt', 'w')
#STAT_FILE = sys.stdout

PUBLIC_FILE_STORAGE = './output/'

################################
###### DATABASE INFO ###########
################################

SQL_PUBLIC_STRING = ("Driver={SQL Server Native Client 11.0};"
    "Server=XXX;"
    "Database=XXX;"
    "Trusted_Connection=yes;")

################################
###### OTHER SETTINGS ##########
################################ 

FIRST_YEAR = 2006  

def month_to_int(monthString):
    if "Enero" in monthString:
        return 1
    elif "Febrero" in monthString:
        return 2
    elif "Marzo" in monthString:
        return 3
    elif "Abril" in monthString:
        return 4
    elif "Mayo" in monthString:
        return 5
    elif "Junio" in monthString:
        return 6
    elif "Julio" in monthString:
        return 7
    elif "Agosto" in monthString:
        return 8
    elif "Septiembre" in monthString:
        return 9
    elif "Octubre" in monthString:
        return 10
    elif "Noviembre" in monthString:
        return 11
    elif "Diciembre" in monthString:
        return 12

def load_public_data():
    global STAT_FILE
    global SQL_PUBLIC_STRING
    global PUBLIC_FILE_STORAGE
    global FIRST_YEAR

    print('Begining load_public_data...', file=STAT_FILE, flush=True)
    storageLoc = PUBLIC_FILE_STORAGE
    
    newRecords = False
    paramsSQLPublic = urllib.parse.quote_plus(SQL_PUBLIC_STRING)

    sqlPublicEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % paramsSQLPublic)
    publicSession = Session(sqlPublicEngine)

    start = time.time()
    page = requests.get('http://wvw.se.gob.ar/datosupstream/consulta_avanzada/listado.php')
    tree = html.fromstring(page.content)
    
    companies = tree.xpath('//select[@name="idempresa"]/option/text()')
    companyValues = tree.xpath('//select[@name="idempresa"]/option/@value')
    companies = companies[1::]
    companyValues = companyValues[1::]
    duration = time.time() - start
    print('Company request total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)


    ##### START OF YEAR LOOP ####
    
    for localYear in range(FIRST_YEAR, dt.datetime.now().year+1):
        ##### START OF COMPANY LOOP ####
        for opr in companyValues:

            start = time.time()    
            r = requests.post("https://www.se.gob.ar/datosupstream/consulta_avanzada/listado.php", 
                data={"idempresa":  opr, 'idmes': '%', 'idanio': localYear, 'submit': 'Ver'})
            tree = html.fromstring(r.content)
            duration = time.time() - start
            print('Request for operator '+str(opr)+' in year '+str(localYear)+' total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

            loadFrame = pd.read_html(r.text)
            loadFrame = loadFrame[0]
            links = tree.xpath('//tr/*//a/@href')

            ##### START OF LINKS LOOP ####
            load_index = -1
            
            for index, row in loadFrame.iterrows():
                if row['Estado'] != 'SIN DATOS':
                    load_index += 1
                    fileName = opr+'-'+str(month_to_int(row['Mes']))+'-'+str(row['Año'])+'.html'
                    fullName = Path(storageLoc+fileName)
                    #check if available files already exist
                    if fullName.is_file():
                        print(fileName, 'already exists', file=STAT_FILE, flush=True)

                    #if not, load file
                    else:
                        

                        start = time.time()   
                        
                        xlFile = requests.get(links[load_index])
                        f = open(storageLoc+fileName,'w', encoding='latin-1', errors='replace')
                        f.write(xlFile.text)
                        f.close() 
                        duration = time.time() - start
                        print('File request, operator '+fileName+' total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

                        #### THIS BLOCK CAN BE UNINDENTED IF ALL FILES NEED TO BE LOADED ####
                        start = time.time() 
                        newRecords = True

                        f = open(storageLoc+fileName,'r', encoding='latin-1')
                        all_data = f.read()
                        if "No se encontraron datos" not in all_data:
                            df = pd.read_html(all_data)
                            f.close()
                            df = df[0]
                            df.columns = df.iloc[0]
                            df = df.drop(df.index[0])
                            tree = html.fromstring(all_data)
                            
                            localMonth = tree.xpath("//font/text()")
                            localMonth = month_to_int(''.join([x for x in localMonth if 'Período' in x]))
                            thisDate = dt.date(localYear,localMonth,1)
                            df = df.assign(fecha=thisDate)
                            thisCompany = companies[companyValues.index(opr)]
                            df = df.assign(operador_actual=thisCompany)
                            
                            df = df.rename(columns={'fecha': 'fecha',
                                'Sigla': 'Sigla',
                                'Cuenca': 'Cuenca',
                                'Provincia': 'Provincia',
                                'Área': 'Area',
                                'Yacimiento': 'Yacimiento',
                                'ID Pozo': 'ID_Pozo',
                                'Form.Prod.': 'Form_Prod',
                                'Cód.Propio': 'Nom_Propio',
                                'Nom.Propio': 'Cod_Propio',
                                'Prod.Men.Pet.(m3)': 'Prod_Men_Pet_m3',
                                'Prod.Men.Gas(Mm3)': 'Prod_Men_Gas_Mm3',
                                'Prod.Men.Agua(m3)': 'Prod_Men_Agua_m3',
                                'Prod.Acum.Pet.(m3)': 'Prod_Acum_Pet_m3',
                                'Prod.Acum.Gas(Mm3)': 'Prod_Acum_Gas_Mm3',
                                'Prod.Acum.Agua(m3)': 'Prod_Acum_Agua_m3',
                                'Iny.Agua(m3)': 'Iny_Agua_m3',
                                'Iny.Gas(Mm3)': 'Iny_Gas_Mm3',
                                'Iny.CO2(Mm3)': 'Iny_CO2_Mm3',
                                'Iny.Otros(m3)': 'Iny_Otros_m3',
                                'RGP': 'RGP',
                                '% de Agua': 'Porc_de_Agua',
                                'TEF': 'TEF',
                                'Vida Útil': 'Vida_Util',
                                'Sist.Extrac.': 'Sist_Extrac',
                                'Est.Pozo': 'Est_Pozo',
                                'Tipo Pozo': 'Tipo_Pozo',
                                'Clasificación': 'Clasificación',
                                'Sub clasificación': 'Sub_Clasificación',
                                'Tipo de Recurso': 'Tipo_de_Recurso',
                                'Sub tipo de Recurso': 'Sub_tipo_de_recurso',
                                'Observaciones': 'Observaciones',
                                'Latitud': 'Latitud',
                                'Longitud': 'Longitud',
                                'Cota': 'Cota',
                                'Profundidad': 'Profundidad',
                                'operador_actual': 'operador_actual'
                            })
                            

                            df = df[['fecha',
                                'Sigla',
                                'Cuenca',
                                'Provincia',
                                'Area',
                                'Yacimiento',
                                'ID_Pozo',
                                'Form_Prod',
                                'Nom_Propio',
                                'Cod_Propio',
                                'Prod_Men_Pet_m3',
                                'Prod_Men_Gas_Mm3',
                                'Prod_Men_Agua_m3',
                                'Prod_Acum_Pet_m3',
                                'Prod_Acum_Gas_Mm3',
                                'Prod_Acum_Agua_m3',
                                'Iny_Agua_m3',
                                'Iny_Gas_Mm3',
                                'Iny_CO2_Mm3',
                                'Iny_Otros_m3',
                                'RGP',
                                'Porc_de_Agua',
                                'TEF',
                                'Vida_Util',
                                'Sist_Extrac',
                                'Est_Pozo',
                                'Tipo_Pozo',
                                'Clasificación',
                                'Sub_Clasificación',
                                'Tipo_de_Recurso',
                                'Sub_tipo_de_recurso',
                                'Observaciones',
                                'Latitud',
                                'Longitud',
                                'Cota',
                                'Profundidad',
                                'operador_actual'
                            ]]

                            df = df.replace(to_replace="NO DISPONIBLE", value=0)

                            df[['Prod_Men_Pet_m3',
                                'Prod_Men_Gas_Mm3',
                                'Prod_Men_Agua_m3',
                                'Prod_Acum_Pet_m3',
                                'Prod_Acum_Gas_Mm3',
                                'Prod_Acum_Agua_m3',
                                'Iny_Agua_m3',
                                'Iny_Gas_Mm3',
                                'Iny_CO2_Mm3',
                                'Iny_Otros_m3',
                                'RGP',
                                'Porc_de_Agua',
                                'TEF',
                                'Vida_Util',
                                'Latitud',
                                'Longitud',
                                'Cota',
                                'Profundidad'
                            ]] = df[['Prod_Men_Pet_m3',
                                'Prod_Men_Gas_Mm3',
                                'Prod_Men_Agua_m3',
                                'Prod_Acum_Pet_m3',
                                'Prod_Acum_Gas_Mm3',
                                'Prod_Acum_Agua_m3',
                                'Iny_Agua_m3',
                                'Iny_Gas_Mm3',
                                'Iny_CO2_Mm3',
                                'Iny_Otros_m3',
                                'RGP',
                                'Porc_de_Agua',
                                'TEF',
                                'Vida_Util',
                                'Latitud',
                                'Longitud',
                                'Cota',
                                'Profundidad'
                            ]].apply(pd.to_numeric)

                            duration = time.time() - start
                            print('Formatting file '+fileName+' total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

                            start = time.time()
                            df.to_sql('CAPITULO_IV_TEMP', sqlPublicEngine, if_exists='append', 
                                chunksize=10,
                                dtype={'fecha': sa.DateTime(),
                                    'Sigla': sa.String(255),
                                    'Cuenca': sa.String(255),
                                    'Provincia': sa.String(255),
                                    'Area': sa.String(255),
                                    'Yacimiento': sa.String(255),
                                    'ID_Pozo': sa.String(255),
                                    'Form_Prod': sa.String(255),
                                    'Nom_Propio': sa.String(255),
                                    'Cod_Propio': sa.String(255),
                                    'Prod_Men_Pet_m3': sa.Float(),
                                    'Prod_Men_Gas_Mm3': sa.Float(),
                                    'Prod_Men_Agua_m3': sa.Float(),
                                    'Prod_Acum_Pet_m3': sa.Float(),
                                    'Prod_Acum_Gas_Mm3': sa.Float(),
                                    'Prod_Acum_Agua_m3': sa.Float(),
                                    'Iny_Agua_m3': sa.Float(),
                                    'Iny_Gas_Mm3': sa.Float(),
                                    'Iny_CO2_Mm3': sa.Float(),
                                    'Iny_Otros_m3': sa.Float(),
                                    'RGP': sa.Float(),
                                    'Porc_de_Agua': sa.Float(),
                                    'TEF': sa.Float(),
                                    'Vida_Util': sa.Float(),
                                    'Sist_Extrac': sa.String(255),
                                    'Est_Pozo': sa.String(255),
                                    'Tipo_Pozo': sa.String(255),
                                    'Clasificación': sa.String(255),
                                    'Sub_Clasificación': sa.String(255),
                                    'Tipo_de_Recurso': sa.String(255),
                                    'Sub_tipo_de_recurso': sa.String(255),
                                    'Observaciones': sa.String(255),
                                    'Latitud': sa.Float(),
                                    'Longitud': sa.Float(),
                                    'Cota': sa.Float(),
                                    'Profundidad': sa.Float(),
                                    'operador_actual': sa.String(255)
                                }, 
                                index=False)
                            duration = time.time() - start
                            print('Append to temp table creation query total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)
                        f.close()
                    #### END OF INDENTABLE BLOCK #### 
            ###### END OF LOOP FOR LINKS #######
        ###### END OF LOOP FOR COMPANIES #######
    ###### END OF LOOP FOR YEARS #######

    if newRecords:
        start = time.time()
        allDataUpdateQuery = text("INSERT INTO CAPITULO_IV (fecha, Sigla, Cuenca, "
            "Provincia, Area, Yacimiento, ID_Pozo, Form_Prod, Nom_Propio, Cod_Propio, "
            "Prod_Men_Pet_m3, Prod_Men_Gas_Mm3, Prod_Men_Agua_m3, Prod_Acum_Pet_m3, "
            "Prod_Acum_Gas_Mm3, Prod_Acum_Agua_m3, Iny_Agua_m3, Iny_Gas_Mm3, Iny_CO2_Mm3, "
            "Iny_Otros_m3, RGP, Porc_de_Agua, TEF, Vida_Util, Sist_Extrac, Est_Pozo, "
            "Tipo_Pozo, Clasificación, Sub_Clasificación, Tipo_de_Recurso, Sub_tipo_de_recurso, "
            "Observaciones, Latitud, Longitud, Cota, Profundidad, operador_actual)"
            "SELECT CAPITULO_IV_TEMP.fecha, CAPITULO_IV_TEMP.Sigla, CAPITULO_IV_TEMP.Cuenca, "
            "CAPITULO_IV_TEMP.Provincia, CAPITULO_IV_TEMP.Area, CAPITULO_IV_TEMP.Yacimiento, "
            "CAPITULO_IV_TEMP.ID_Pozo, CAPITULO_IV_TEMP.Form_Prod, CAPITULO_IV_TEMP.Nom_Propio, "
            "CAPITULO_IV_TEMP.Cod_Propio, CAPITULO_IV_TEMP.Prod_Men_Pet_m3, CAPITULO_IV_TEMP.Prod_Men_Gas_Mm3, "
            "CAPITULO_IV_TEMP.Prod_Men_Agua_m3, CAPITULO_IV_TEMP.Prod_Acum_Pet_m3, "
            "CAPITULO_IV_TEMP.Prod_Acum_Gas_Mm3, CAPITULO_IV_TEMP.Prod_Acum_Agua_m3, "
            "CAPITULO_IV_TEMP.Iny_Agua_m3, CAPITULO_IV_TEMP.Iny_Gas_Mm3, CAPITULO_IV_TEMP.Iny_CO2_Mm3, "
            "CAPITULO_IV_TEMP.Iny_Otros_m3, CAPITULO_IV_TEMP.RGP, CAPITULO_IV_TEMP.Porc_de_Agua, "
            "CAPITULO_IV_TEMP.TEF, CAPITULO_IV_TEMP.Vida_Util, CAPITULO_IV_TEMP.Sist_Extrac, "
            "CAPITULO_IV_TEMP.Est_Pozo, CAPITULO_IV_TEMP.Tipo_Pozo, CAPITULO_IV_TEMP.Clasificación, "
            "CAPITULO_IV_TEMP.Sub_Clasificación, CAPITULO_IV_TEMP.Tipo_de_Recurso, CAPITULO_IV_TEMP.Sub_tipo_de_recurso, "
            "CAPITULO_IV_TEMP.Observaciones, CAPITULO_IV_TEMP.Latitud, CAPITULO_IV_TEMP.Longitud, "
            "CAPITULO_IV_TEMP.Cota, CAPITULO_IV_TEMP.Profundidad, CAPITULO_IV_TEMP.operador_actual "
            "FROM CAPITULO_IV_TEMP LEFT JOIN CAPITULO_IV "
            "ON ((CAPITULO_IV_TEMP.fecha = CAPITULO_IV.fecha) AND "
            "(CAPITULO_IV_TEMP.ID_Pozo = CAPITULO_IV.ID_Pozo)) "
            "WHERE ((CAPITULO_IV.fecha IS NULL) AND (CAPITULO_IV.ID_Pozo IS NULL));")

        publicSession.execute(allDataUpdateQuery)
        publicSession.commit()
        duration = time.time() - start
        print('Public data append total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

        start = time.time()
        dupilcateCleanupQuery = text("WITH cte AS ( "
            "SELECT[ID_Pozo], [fecha], "
            "    row_number() OVER(PARTITION BY ID_Pozo, fecha ORDER BY fecha) AS [rn] "
            "FROM CAPITULO_IV "
            ") "
            "DELETE cte WHERE [rn] > 1;")

        publicSession.execute(dupilcateCleanupQuery)
        publicSession.commit()
        duration = time.time() - start
        print('Public data duplicate remove total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

        start = time.time()
        dropTableQuery = text("DROP TABLE CAPITULO_IV_TEMP")
        publicSession.execute(dropTableQuery)
        publicSession.commit()
        duration = time.time() - start
        print('Table cleanup total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    publicSession.close()

    print('Ending load_public_data...', file=STAT_FILE, flush=True)
    return 0

def load_public_well_headers():
    global STAT_FILE
    global SQL_PUBLIC_STRING

    print('Begining load_public_well_headers...', file=STAT_FILE, flush=True)

    fileLocation = 'http://datos.minem.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/cbfa4d79-ffb3-4096-bab5-eb0dde9a8385/download/listado-de-pozos-cargados-por-empresas-operadoras.csv'

    paramsSQLPublic = urllib.parse.quote_plus(SQL_PUBLIC_STRING)

    sqlPublicEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % paramsSQLPublic)
    publicSession = Session(sqlPublicEngine)

    start = time.time()
    page = requests.get('http://wvw.se.gob.ar/datosupstream/consulta_avanzada/listado.php')
    tree = html.fromstring(page.content)
    
    companies = tree.xpath('//select[@name="idempresa"]/option/text()')
    companyValues = tree.xpath('//select[@name="idempresa"]/option/@value')
    companies = companies[1::]
    companyValues = companyValues[1::]
    compDf = pd.DataFrame(
        {'idempresa' : companyValues,
        'empresa_nombre' : companies
        })
    duration = time.time() - start
    print('Company request total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    df = pd.read_csv(fileLocation)
    duration = time.time() - start
    print('Reader data request time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    df_all = df.merge(compDf, on=['idempresa'], 
                   how='left', indicator=False)
    
    df_all = df_all.replace(to_replace="2999-12-30", value=np.nan)

    df_all[['adjiv_fecha_inicio',
        'adjiv_fecha_fin',
        'adjiv_fecha_inicio_term',
        'adjiv_fecha_fin_term',
        'adjiv_fecha_abandono',
        'fechadeingreso',
        'fecha_data'
        ]].apply(pd.to_datetime)
    
    df_all[['coordenadax',
        'coordenaday',
        'cota',
        'profundidad',
        'pet_inicial',
        'gas_inicial',
        'agua_inicial',
        'iny_agua_inicial',
        'iny_gas_inicial',
        'iny_otros_inicial',
        'iny_co2_inicial',
        'vida_util_inicial',
        'adjiv_capacidad_perf',
        'adjiv_tipo_reservorio',
        'adjiv_subtipo_reservorio',
        'adjiv_comp_perf',
        'petroleo',
        'gas',
        'agua',
        'periodo'
        ]].apply(pd.to_numeric)


    df_all.to_sql('LISTADO_DE_POZOS_TEMP', sqlPublicEngine, if_exists='append', 
        chunksize=10,
        dtype={'idpozo' : sa.String(255) ,
                'sigla' : sa.String(255) ,
                'formprod' : sa.String(255) ,
                'idempresa' : sa.String(255) ,
                'idareapermisoconcesion' : sa.String(255) ,
                'idareayacimiento' : sa.String(255) ,
                'idcuenca' : sa.String(255) ,
                'idprovincia' : sa.String(255) ,
                'codigopropio' : sa.String(255) ,
                'nombrepropio' : sa.String(255) ,
                'coordenadax' : sa.Float() ,
                'coordenaday' : sa.Float() ,
                'cota' : sa.Float() ,
                'profundidad' : sa.Float() ,
                'pet_inicial' : sa.Float() ,
                'gas_inicial' : sa.Float() ,
                'agua_inicial' : sa.Float() ,
                'iny_agua_inicial' : sa.Float() ,
                'iny_gas_inicial' : sa.Float() ,
                'iny_otros_inicial' : sa.Float() ,
                'iny_co2_inicial' : sa.Float() ,
                'vida_util_inicial' : sa.Float() ,
                'adjiv_fecha_inicio' : sa.DateTime() ,
                'adjiv_equipo_utilizar' : sa.String(255) ,
                'adjiv_capacidad_perf' : sa.Float() ,
                'adjiv_tipo_reservorio' : sa.BigInteger() ,
                'adjiv_subtipo_reservorio' : sa.BigInteger() ,
                'adjiv_fecha_fin' : sa.DateTime() ,
                'adjiv_fecha_inicio_term' : sa.DateTime() ,
                'adjiv_fecha_fin_term' : sa.DateTime() ,
                'adjiv_fecha_abandono' : sa.DateTime() ,
                'adjiv_clasificacion' : sa.String(255) ,
                'adjiv_subclasificacion' : sa.String(255) ,
                'fechadeingreso' : sa.String(255),
                'adjiv_comp_perf' : sa.BigInteger() ,
                'unique_sigla_formprod': sa.String(255) ,
                'areapermisoconcesion' : sa.String(255) ,
                'areayacimiento' : sa.String(255) ,
                'cuenca' : sa.String(255) ,
                'provincia' : sa.String(255) ,
                'petroleo' : sa.Float() ,
                'gas' : sa.Float() ,
                'agua' : sa.Float() ,
                'periodo' : sa.Float() ,
                'clasificacion' : sa.String(255) ,
                'subclasificacion' : sa.String(255) ,
                'tipo_reservorio' : sa.String(255) ,
                'subtipo_reservorio' : sa.String(255) ,
                'comp_perf' : sa.String(255) ,
                'gasplus' : sa.String(255) ,
                'fecha_data' : sa.String(255),
                'empresa_nombre' : sa.String(255)

        }, 
        index=False)

    duration = time.time() - start
    print('Upload temporary table time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    deleteDuplicateQuery = text("delete x "
        "from ( "
        "select *, rn=row_number() over (partition by idpozo order by idpozo) "
        "from LISTADO_DE_POZOS_TEMP "
        ") x "
        "where rn > 1;")
    publicSession.execute(deleteDuplicateQuery)
    duration = time.time() - start
    print('Header duplicate cleanup query total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    headerInsertQuery = text("INSERT INTO LISTADO_DE_POZOS "
                         "(idpozo, sigla, formprod, idempresa, idareapermisoconcesion, "
                         "idareayacimiento, idcuenca, idprovincia, codigopropio, nombrepropio, "
                         "coordenadax, coordenaday, cota, profundidad, pet_inicial, gas_inicial, agua_inicial, "
                         "iny_agua_inicial, iny_gas_inicial, iny_otros_inicial, iny_co2_inicial, vida_util_inicial, "
                         "adjiv_fecha_inicio, adjiv_equipo_utilizar, adjiv_capacidad_perf, adjiv_tipo_reservorio, "
                         "adjiv_subtipo_reservorio, adjiv_fecha_fin, "
                         "adjiv_fecha_inicio_term, adjiv_fecha_fin_term, adjiv_fecha_abandono, adjiv_clasificacion, "
                         "adjiv_subclasificacion, fechadeingreso, adjiv_comp_perf, unique_sigla_formprod, areapermisoconcesion, "
                         "areayacimiento, cuenca, "
                         "provincia, petroleo, gas, agua, periodo, clasificacion, subclasificacion, tipo_reservorio, "
                         "subtipo_reservorio, comp_perf, gasplus, fecha_data, empresa_nombre) "
                         "SELECT LISTADO_DE_POZOS_TEMP.idpozo, LISTADO_DE_POZOS_TEMP.sigla, LISTADO_DE_POZOS_TEMP.formprod, LISTADO_DE_POZOS_TEMP.idempresa, "
                         "LISTADO_DE_POZOS_TEMP.idareapermisoconcesion, LISTADO_DE_POZOS_TEMP.idareayacimiento, "
                         "LISTADO_DE_POZOS_TEMP.idcuenca, LISTADO_DE_POZOS_TEMP.idprovincia, LISTADO_DE_POZOS_TEMP.codigopropio, LISTADO_DE_POZOS_TEMP.nombrepropio, "
                         "LISTADO_DE_POZOS_TEMP.coordenadax, LISTADO_DE_POZOS_TEMP.coordenaday, "
                         "LISTADO_DE_POZOS_TEMP.cota, LISTADO_DE_POZOS_TEMP.profundidad, LISTADO_DE_POZOS_TEMP.pet_inicial, LISTADO_DE_POZOS_TEMP.gas_inicial, "
                         "LISTADO_DE_POZOS_TEMP.agua_inicial, LISTADO_DE_POZOS_TEMP.iny_agua_inicial, "
                         "LISTADO_DE_POZOS_TEMP.iny_gas_inicial, LISTADO_DE_POZOS_TEMP.iny_otros_inicial, LISTADO_DE_POZOS_TEMP.iny_co2_inicial, "
                         "LISTADO_DE_POZOS_TEMP.vida_util_inicial, LISTADO_DE_POZOS_TEMP.adjiv_fecha_inicio, "
                         "LISTADO_DE_POZOS_TEMP.adjiv_equipo_utilizar, LISTADO_DE_POZOS_TEMP.adjiv_capacidad_perf, LISTADO_DE_POZOS_TEMP.adjiv_tipo_reservorio, "
                         "LISTADO_DE_POZOS_TEMP.adjiv_subtipo_reservorio, LISTADO_DE_POZOS_TEMP.adjiv_fecha_fin, "
                         "LISTADO_DE_POZOS_TEMP.adjiv_fecha_inicio_term, LISTADO_DE_POZOS_TEMP.adjiv_fecha_fin_term, LISTADO_DE_POZOS_TEMP.adjiv_fecha_abandono, "
                         "LISTADO_DE_POZOS_TEMP.adjiv_clasificacion, "
                         "LISTADO_DE_POZOS_TEMP.adjiv_subclasificacion, LISTADO_DE_POZOS_TEMP.fechadeingreso, LISTADO_DE_POZOS_TEMP.adjiv_comp_perf, "
                         "LISTADO_DE_POZOS_TEMP.unique_sigla_formprod, LISTADO_DE_POZOS_TEMP.areapermisoconcesion, "
                         "LISTADO_DE_POZOS_TEMP.areayacimiento, LISTADO_DE_POZOS_TEMP.cuenca, LISTADO_DE_POZOS_TEMP.provincia, LISTADO_DE_POZOS_TEMP.petroleo, "
                         "LISTADO_DE_POZOS_TEMP.gas, LISTADO_DE_POZOS_TEMP.agua, LISTADO_DE_POZOS_TEMP.periodo, "
                         "LISTADO_DE_POZOS_TEMP.clasificacion, LISTADO_DE_POZOS_TEMP.subclasificacion, LISTADO_DE_POZOS_TEMP.tipo_reservorio, "
                         "LISTADO_DE_POZOS_TEMP.subtipo_reservorio, LISTADO_DE_POZOS_TEMP.comp_perf, LISTADO_DE_POZOS_TEMP.gasplus, "
                         "LISTADO_DE_POZOS_TEMP.fecha_data, LISTADO_DE_POZOS_TEMP.empresa_nombre "
                         "FROM LISTADO_DE_POZOS RIGHT OUTER JOIN "
                         "LISTADO_DE_POZOS_TEMP ON LISTADO_DE_POZOS.idpozo = LISTADO_DE_POZOS_TEMP.idpozo "
                         "WHERE (LISTADO_DE_POZOS.idpozo IS NULL);")
    
    publicSession.execute(headerInsertQuery)
    duration = time.time() - start
    print('Header data update query total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    dropTableQuery = text("DROP TABLE LISTADO_DE_POZOS_TEMP")
    publicSession.execute(dropTableQuery)
    publicSession.commit()
    duration = time.time() - start
    print('Table cleanup total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)


    publicSession.close()

    print('Ending load_public_well_headers...', file=STAT_FILE, flush=True)

    return 0

def load_public_completion():
    global STAT_FILE
    global SQL_PUBLIC_STRING

    print('Begining load_public_completion...', file=STAT_FILE, flush=True)

    fileLocation = 'http://datos.minem.gob.ar/dataset/71fa2e84-0316-4a1b-af68-7f35e41f58d7/resource/2280ad92-6ed3-403e-a095-50139863ab0d/download/datos-de-fractura-de-pozos-de-hidrocarburos-adjunto-iv-actualizacin-diaria.csv'
    
    paramsSQLPublic = urllib.parse.quote_plus(SQL_PUBLIC_STRING)

    sqlPublicEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % paramsSQLPublic)
    publicSession = Session(sqlPublicEngine)

    start = time.time()
    df = pd.read_csv(fileLocation)
    duration = time.time() - start
    print('Reader data request time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)
    

    df[['fecha_inicio_fractura',
        'fecha_fin_fractura',
        'fecha_data'
        ]] = df[['fecha_inicio_fractura',
        'fecha_fin_fractura',
        'fecha_data'
        ]].apply(pd.to_datetime)
    
    df[[
        'longitud_rama_horizontal_m',
        'cantidad_fracturas',
        'arena_bombeada_nacional_tn',
        'arena_bombeada_importada_tn',
        'agua_inyectada_m3',
        'co2_inyectado_m3',
        'presion_maxima_psi',
        'potencia_equipos_fractura_hp',
        'mes'
        ]] = df[[
        'longitud_rama_horizontal_m',
        'cantidad_fracturas',
        'arena_bombeada_nacional_tn',
        'arena_bombeada_importada_tn',
        'agua_inyectada_m3',
        'co2_inyectado_m3',
        'presion_maxima_psi',
        'potencia_equipos_fractura_hp',
        'mes'
        ]].apply(pd.to_numeric)

    df.to_sql('DATOS_FRACTURAS_TEMP', sqlPublicEngine, if_exists='append', 
        chunksize=10,
        dtype={'id_base_fractura_adjiv': sa.String(255),
            'idpozo': sa.String(255),
            'sigla': sa.String(255),
            'areapermisoconcesion': sa.String(255),
            'yacimiento': sa.String(255),
            'formacion_productiva': sa.String(255),
            'tipo_reservorio': sa.String(255),
            'subtipo_reservorio': sa.String(255),
            'longitud_rama_horizontal_m': sa.Float(),
            'cantidad_fracturas': sa.BigInteger(),
            'tipo_terminacion': sa.String(255),
            'arena_bombeada_nacional_tn': sa.Float(),
            'arena_bombeada_importada_tn': sa.Float(),
            'agua_inyectada_m3': sa.Float(),
            'co2_inyectado_m3': sa.Float(),
            'presion_maxima_psi': sa.Float(),
            'potencia_equipos_fractura_hp': sa.Float(),
            'fecha_inicio_fractura': sa.DateTime(),
            'fecha_fin_fractura': sa.DateTime(),
            'fecha_data': sa.DateTime(),
            'empresa_informante': sa.String(255),
            'mes': sa.Integer()
        }, 
        index=False)

    duration = time.time() - start
    print('Upload temporary table time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)
    
    start = time.time()
    headerInsertQuery = text("INSERT INTO DATOS_FRACTURAS "
        "(id_base_fractura_adjiv, idpozo, sigla, areapermisoconcesion, yacimiento, "
        "formacion_productiva, tipo_reservorio, subtipo_reservorio, longitud_rama_horizontal_m, "
        "cantidad_fracturas, tipo_terminacion, arena_bombeada_nacional_tn, arena_bombeada_importada_tn, "
        "agua_inyectada_m3, co2_inyectado_m3, presion_maxima_psi, fecha_inicio_fractura, fecha_fin_fractura, "
        "fecha_data, empresa_informante, mes) "
        "SELECT DATOS_FRACTURAS_TEMP.id_base_fractura_adjiv, DATOS_FRACTURAS_TEMP.idpozo, DATOS_FRACTURAS_TEMP.sigla, "
        "DATOS_FRACTURAS_TEMP.areapermisoconcesion, DATOS_FRACTURAS_TEMP.yacimiento, DATOS_FRACTURAS_TEMP.formacion_productiva, "
        "DATOS_FRACTURAS_TEMP.tipo_reservorio, DATOS_FRACTURAS_TEMP.subtipo_reservorio, DATOS_FRACTURAS_TEMP.longitud_rama_horizontal_m, "
        "DATOS_FRACTURAS_TEMP.cantidad_fracturas, DATOS_FRACTURAS_TEMP.tipo_terminacion, "
        "DATOS_FRACTURAS_TEMP.arena_bombeada_nacional_tn, DATOS_FRACTURAS_TEMP.arena_bombeada_importada_tn, "
        "DATOS_FRACTURAS_TEMP.agua_inyectada_m3, DATOS_FRACTURAS_TEMP.co2_inyectado_m3, DATOS_FRACTURAS_TEMP.presion_maxima_psi, "
        "DATOS_FRACTURAS_TEMP.fecha_inicio_fractura, DATOS_FRACTURAS_TEMP.fecha_fin_fractura, "
        "DATOS_FRACTURAS_TEMP.fecha_data, DATOS_FRACTURAS_TEMP.empresa_informante, DATOS_FRACTURAS_TEMP.mes "
        "FROM DATOS_FRACTURAS RIGHT OUTER JOIN "
        "DATOS_FRACTURAS_TEMP ON DATOS_FRACTURAS.id_base_fractura_adjiv  = DATOS_FRACTURAS_TEMP.id_base_fractura_adjiv "
        "WHERE (DATOS_FRACTURAS.id_base_fractura_adjiv IS NULL);")
    
    publicSession.execute(headerInsertQuery)
    duration = time.time() - start
    print('Header data update query total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    start = time.time()
    dropTableQuery = text("DROP TABLE DATOS_FRACTURAS_TEMP ")
    publicSession.execute(dropTableQuery)
    publicSession.commit()
    duration = time.time() - start
    print('Table cleanup total time: {:.2f} seconds'.format(duration), file=STAT_FILE, flush=True)

    
    publicSession.close()

    print('Ending load_public_completion...', file=STAT_FILE, flush=True)

    return 0

if __name__ == '__main__':  
    global_start = time.time()
    print('Database update starting at:', dt.date.today(), file=STAT_FILE, flush=True)

    load_public_data()
    load_public_well_headers()
    load_public_completion() 

    global_duration = time.time() - global_start
    print('Database updates total time: {:.2f} seconds'.format(global_duration), file=STAT_FILE, flush=True)
    STAT_FILE.close()
