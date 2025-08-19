from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='geonombreFundar == "Argentina"'),
	rename_cols(map={'tipo_idh': 'categoria'}),
	query(condition='anio in [1990, 2022]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13596 entries, 0 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  13530 non-null  object 
#   1   geonombreFundar  13530 non-null  object 
#   2   anio             13596 non-null  int64  
#   3   tipo_idh         13596 non-null  object 
#   4   country          13596 non-null  object 
#   5   valor            11271 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | tipo_idh   | country     |   valor |
#  |---:|:------------------|:------------------|-------:|:-----------|:------------|--------:|
#  |  0 | AFG               | Afganistán        |   1990 | IDH        | Afghanistan |   0.284 |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 66 entries, 330 to 395
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  66 non-null     object 
#   1   geonombreFundar  66 non-null     object 
#   2   anio             66 non-null     int64  
#   3   tipo_idh         66 non-null     object 
#   4   country          66 non-null     object 
#   5   valor            66 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | tipo_idh   | country   |   valor |
#  |----:|:------------------|:------------------|-------:|:-----------|:----------|--------:|
#  | 330 | ARG               | Argentina         |   1990 | IDH        | Argentina |   0.724 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_idh': 'categoria'})
#  Index: 66 entries, 330 to 395
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  66 non-null     object 
#   1   geonombreFundar  66 non-null     object 
#   2   anio             66 non-null     int64  
#   3   categoria        66 non-null     object 
#   4   country          66 non-null     object 
#   5   valor            66 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | categoria   | country   |   valor |
#  |----:|:------------------|:------------------|-------:|:------------|:----------|--------:|
#  | 330 | ARG               | Argentina         |   1990 | IDH         | Argentina |   0.724 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1990, 2022]')
#  Index: 4 entries, 330 to 395
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4 non-null      object 
#   1   geonombreFundar  4 non-null      object 
#   2   anio             4 non-null      int64  
#   3   categoria        4 non-null      object 
#   4   country          4 non-null      object 
#   5   valor            4 non-null      float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | categoria   | country   |   valor |
#  |----:|:------------------|:------------------|-------:|:------------|:----------|--------:|
#  | 330 | ARG               | Argentina         |   1990 | IDH         | Argentina |   0.724 |
#  
#  ------------------------------
#  