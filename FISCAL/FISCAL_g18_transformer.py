from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='finalidad_desc', replacements={'DEUDA PUBLICA': 'Deuda pública', 'SERVICIOS ECONOMICOS': 'Servicios económicos', 'SERVICIOS SOCIALES': 'Servicios sociales', 'SERVICIOS DE DEFENSA Y SEGURIDAD': 'Defensa y seguridad', 'ADMINISTRACION GUBERNAMENTAL': 'Adm. gubernamental'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     105 non-null    int64  
#   1   finalidad_desc           105 non-null    object 
#   2   total_credito_devengado  105 non-null    float64
#  
#  |    |   anio | finalidad_desc   |   total_credito_devengado |
#  |---:|-------:|:-----------------|--------------------------:|
#  |  0 |   2004 | DEUDA PUBLICA    |                   1.21374 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='finalidad_desc', replacements={'DEUDA PUBLICA': 'Deuda pública', 'SERVICIOS ECONOMICOS': 'Servicios económicos', 'SERVICIOS SOCIALES': 'Servicios sociales', 'SERVICIOS DE DEFENSA Y SEGURIDAD': 'Defensa y seguridad', 'ADMINISTRACION GUBERNAMENTAL': 'Adm. gubernamental'})
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     105 non-null    int64  
#   1   finalidad_desc           105 non-null    object 
#   2   total_credito_devengado  105 non-null    float64
#  
#  |    |   anio | finalidad_desc   |   total_credito_devengado |
#  |---:|-------:|:-----------------|--------------------------:|
#  |  0 |   2004 | Deuda pública    |                   1.21374 |
#  
#  ------------------------------
#  