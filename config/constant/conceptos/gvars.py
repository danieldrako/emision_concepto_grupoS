import re

query = """
SELECT
fp.tfdUUID, 
maptostring(fp.Conceptos) as 'Conceptos'
FROM DocumentDB.FacturaPersistida fp  
WHERE fp.tfdUUID NOT IN (SELECT tc.'uuid' FROM DEV_FACTURACION.test_conceptos tc)
LIMIT 100000;
"""
cols_=['tfdUUID', 'Conceptos']

output_file = r"FROM_Vertica_Conceptos.csv"

tables_dest = {"conceptos":"test_conceptos"}

schema = "DEV_FACTURACION"
