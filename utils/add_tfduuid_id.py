import os

# Obtiene la ruta del directorio actual donde se encuentra el script
ruta_script = os.path.abspath(__file__)

def add_tfduuid_id(tfduuid, extracted_dicts, new_name_id = ""):
    try:
        dicts_with_id = []
        for ind, dict_ in enumerate(extracted_dicts):
            if isinstance(dict_, dict):
                if new_name_id == "":
                    dicts_with_id.append({"tfduuid": tfduuid, "id": ind+1, **dict_})  
                else:
                    dicts_with_id.append({"tfduuid": tfduuid, new_name_id: ind+1, **dict_})  
                
        return dicts_with_id
    except Exception as e:
        print("ERROR")
        print("Check: ", ruta_script)
        print("Error: ", e)