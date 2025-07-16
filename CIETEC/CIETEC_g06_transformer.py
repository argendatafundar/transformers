from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'ultimo_anio_disponible', 'fuente'], axis=1),
	replace_multiple_values(col='geonombreFundar', replacements={'América Latina y el Caribe': 'A. Latina y el Caribe', 'Países miembros de OCDE': 'Miembros de OCDE', 'Unión Europea (27 países)': 'Unión Europea'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 49 entries, 0 to 48
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         49 non-null     object 
#   1   geonombreFundar         49 non-null     object 
#   2   ultimo_anio_disponible  49 non-null     int64  
#   3   gasto_investigador      49 non-null     float64
#   4   fuente                  49 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   gasto_investigador | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|---------------------:|:---------|
#  |  0 | USA               | Estados Unidos    |                     2022 |              539.315 | OECD     |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'ultimo_anio_disponible', 'fuente'], axis=1)
#  RangeIndex: 49 entries, 0 to 48
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geonombreFundar     49 non-null     object 
#   1   gasto_investigador  49 non-null     float64
#  
#  |    | geonombreFundar   |   gasto_investigador |
#  |---:|:------------------|---------------------:|
#  |  0 | Estados Unidos    |              539.315 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='geonombreFundar', replacements={'América Latina y el Caribe': 'A. Latina y el Caribe', 'Países miembros de OCDE': 'Miembros de OCDE', 'Unión Europea (27 países)': 'Unión Europea'})
#  RangeIndex: 49 entries, 0 to 48
#  Data columns (total 2 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geonombreFundar     49 non-null     object 
#   1   gasto_investigador  49 non-null     float64
#  
#  |    | geonombreFundar   |   gasto_investigador |
#  |---:|:------------------|---------------------:|
#  |  0 | Estados Unidos    |              539.315 |
#  
#  ------------------------------
#  