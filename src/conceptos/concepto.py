import sys
import os
#os.chdir("E:\Desarrollos\extract_json_vertica_python\\app")
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../..'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)

import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from datetime import datetime


from utils.re_build_df import re_build_df
from tools.concepto import transform_jsonrow as tjr
from config.constant.conceptos import gvars as gv
from config.constant.conceptos import concepto_vars as conv

from utils.mytime import get_my_time
from data.post_data import post_result
#!##########################################Llamado a la base de datos############################################
#!########################################## Fuente de los datos  ################################################
#Variables

cols_= gv.cols_
output_file = conv.output_file
regular_exp = conv.regular_exp
table_name = gv.tables_dest["conceptos"]
schema = gv.schema


#!#################################################################################################################

# #*##################################GuardarData frame procesado en local##########################################



def concepto(next_step, conceptos_df, carpeta):
    print("_"*80)
    print("\/"*60)
    print("="*80)
    print("")
    print("==============================| Concepto |=============================")
    print("====================================| Processing |===================================")
    print("_"*80)
    print("/\\"*60)
    print("="*80)
      
    start = datetime.now()
    concepto_dict = {"schema":gv.schema, "table":table_name, "process_saved":False, "file":"no-file", "updated":False} 
    cconceptos_df=conceptos_df.copy()
    if next_step:
        
        concepto = tjr.functionTwo_ConceptoCF(cconceptos_df)
        concepto_df = pd.DataFrame(concepto)
        concepto_reb_df = re_build_df(concepto_df, conv.ordered_cols, conv.float_cols,
                                    conv.str_cols, conv.int_cols, new_name_tfduuid=conv.new_name_tfduuid,
                                    to_delete=conv.to_delete)
        #concepto_reb_df = concepto_reb_df[concepto_reb_df['Descripcion'] != '']

        print(concepto_reb_df.head())
        print(concepto_reb_df.shape)


        name_path_concepto = conv.csv_file_path_to_post
        name_path_concepto = carpeta+"/"+name_path_concepto
        my_date = get_my_time()
        exten = ".csv"
        file_to_post_concepto = name_path_concepto+"_"+my_date+exten
        concepto_reb_df.to_csv(file_to_post_concepto, index=False, sep='|')
        end_proces = datetime.now()
        concepto_dict["saved"]  = True
        concepto_dict["file"]  = file_to_post_concepto
        
        print("*************************************************************************************")
        print(f"Time taken in (hh:mm:ss.ms) to process and save datas {file_to_post_concepto} is {end_proces - start}")
        print(f"====================|data to concepto saved on {file_to_post_concepto} file|========================")
        print("*************************************************************************************")
        print("\n"*3)
    else:
        print("No data to process on concepto")
######################################################################################################*####
###################################################################################################?

        
    if concepto_reb_df is not None and not concepto_reb_df.empty:
        print(f"==============================| Uploading on  {schema}.{table_name} |=============================")
        try:
            start_up_concepto = datetime.now()
            post_result(schema, table_name, file_to_post_concepto)
            end_up_concepto  = datetime.now()
            print("==============================| concepto  finished |=============================")
            print("*************************************************************************************")
            print(f"Time taken in (hh:mm:ss.ms) to upload Concepto is {end_up_concepto - start_up_concepto}")
            print("*************************************************************************************")
            print("\n"*3)
            concepto_dict["updated"] = True
        except Exception as e:
            print("Error: ", e)
    else:
        print("==> ==> ==> No data to post on concepto <== <== <==")
    
    
    return concepto_dict
        
