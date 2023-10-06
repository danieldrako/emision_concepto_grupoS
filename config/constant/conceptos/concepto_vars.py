output_file = r"temp/"

test_key = "_Descripcion"

str_cols = [ 'uuid', 'ClaveProdServ', 'NoIdentificacion','ClaveUnidad', 'Unidad', 'Descripcion', 'ObjetoImp'  ]

float_cols = [ 'ValorUnitario', 'Importe', 'Descuento'  ]

int_cols = [ 'idConcepto', 'Cantidad'  ]

ordered_cols = ['uuid',  'idConcepto', "ClaveProdServ", "NoIdentificacion", "Cantidad", "ClaveUnidad"
            , "Unidad","Descripcion" ,"ValorUnitario", "Importe", "Descuento", "ObjetoImp"]

new_name_tfduuid = "uuid"
new_name_id = "idConcepto"

to_delete = "uuid"

regular_exp = r'' #sirve para saber que este elemento esté dentro del json principal

csv_file_path_to_post = "concepto"

table_name = "test_concepto"  # Nombre de la tabla en Vertica

schema = "DEV_FACTURACION"  # Esquema en Vertica