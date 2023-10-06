import sys
import os
# Obtener la ruta absoluta del directorio actual donde se encuentra retencionDR.py
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta absoluta del directorio principal (un nivel arriba)
directorio_principal = os.path.abspath(os.path.join(directorio_actual, '../../'))
# Agregar la ruta al directorio principal al sys.path
sys.path.append(directorio_principal)

import pandas as pd
from tqdm import tqdm
import json
from pandas import json_normalize
tqdm.pandas()
from config.constant.conceptos import  concepto_vars as cvars
from utils.add_tfduuid_id import add_tfduuid_id
from utils.find_dicts_with_keys import find_dicts_with_keys
from utils.extract_keys_from_dicts import extract_keys_from_dicts



#!##################################Función que procesa el diccionario anidado#?####################################
def functionOne_ConceptoCF(row):
    tfduuid = row['tfdUUID']
    try:
        tfduuid = row['tfdUUID']
        conceptos_Text = row['Conceptos']
        conceptos_Json = json.loads(conceptos_Text)
        keys_to_find = ["_ClaveProdServ", "_NoIdentificacion", "_Cantidad", "_ClaveUnidad","_Unidad",
                        "_Descripcion","_ValorUnitario", "_Importe", "_Descuento", "_ObjetoImp"]
        keys_to_extract = keys_to_find.copy()
        result_dicts = find_dicts_with_keys(conceptos_Json, keys_to_find, cvars.test_key)
        extracted_dicts = extract_keys_from_dicts(result_dicts, keys_to_extract)
        result = add_tfduuid_id(tfduuid, extracted_dicts, cvars.new_name_id)
    except Exception as e:
        default_values = {
            "_ClaveProdServ":"00",
            "_NoIdentificacion":"00",
            "_Cantidad":"00",
            "_ClaveUnidad":"00",
            "_Unidad":"00",
            "_Descripcion":"00",
            "_ValorUnitario":00,
            "_Importe":00,
            "_Descuento":00,
            "_ObjetoImp":"00"  
        }
        result = [{'tfduuid': tfduuid, 'idConcepto': 00, **default_values}]
    
    return result
#!###########################################################################?####################################

#?##################################Función sobre los renglones del dataframe#?####################################
def functionTwo_ConceptoCF(df):
    results = df.progress_apply(functionOne_ConceptoCF, axis=1)
    #results = df.apply(functionOne_ConceptoCF, axis=1)
    flat_list = [item for sublist in results for item in sublist]
    return flat_list

#?#####################################################################?##########################################
