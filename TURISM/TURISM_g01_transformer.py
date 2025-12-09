from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='indicador', replacements={'Valor agregado bruto directo turístico (VABDT)': 'PIB turístico directo', 'Valor agregado bruto de las industrias turísticas (VABIT)': 'VAB de las industrias turísticas'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   indicador      14 non-null     object 
#   1   anio           14 non-null     int64  
#   2   valor          14 non-null     float64
#   3   unidad_medida  14 non-null     object 
#  
#  |    | indicador                                      |   anio |     valor | unidad_medida       |
#  |---:|:-----------------------------------------------|-------:|----------:|:--------------------|
#  |  0 | Valor agregado bruto directo turístico (VABDT) |   2016 | 0.0177544 | en % sobre el total |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='indicador', replacements={'Valor agregado bruto directo turístico (VABDT)': 'PIB turístico directo', 'Valor agregado bruto de las industrias turísticas (VABIT)': 'VAB de las industrias turísticas'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   indicador      14 non-null     object 
#   1   anio           14 non-null     int64  
#   2   valor          14 non-null     float64
#   3   unidad_medida  14 non-null     object 
#  
#  |    | indicador             |   anio |     valor | unidad_medida       |
#  |---:|:----------------------|-------:|----------:|:--------------------|
#  |  0 | PIB turístico directo |   2016 | 0.0177544 | en % sobre el total |
#  
#  ------------------------------
#  