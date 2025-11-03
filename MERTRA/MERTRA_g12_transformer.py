from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'provincia_id'], axis=1),
	multiplicar_por_escalar(col='tasa_empleo_18_65', k=100),
	pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geocodigoFundar                 24 non-null     object 
#   1   geonombreFundar                 24 non-null     object 
#   2   provincia_id                    24 non-null     int64  
#   3   establecimientos_cada_1000_hab  24 non-null     float64
#   4   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   provincia_id |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:------------------|:------------------|---------------:|---------------------------------:|--------------------:|
#  |  0 | AR-C              | CABA              |              2 |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'provincia_id'], axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geonombreFundar                 24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | geonombreFundar   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:------------------|---------------------------------:|--------------------:|
#  |  0 | CABA              |                          50.0578 |             78.9682 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo_18_65', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geonombreFundar                 24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | geonombreFundar   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:------------------|---------------------------------:|--------------------:|
#  |  0 | CABA              |                          50.0578 |             78.9682 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   variable         48 non-null     object 
#   2   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | variable                       |   valor |
#  |---:|:------------------|:-------------------------------|--------:|
#  |  0 | CABA              | establecimientos_cada_1000_hab | 50.0578 |
#  
#  ------------------------------
#  