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
	rename_cols(map={'idh_tipo': 'categoria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 40788 entries, 0 to 40787
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  40590 non-null  object 
#   1   geonombreFundar  40590 non-null  object 
#   2   country          40788 non-null  object 
#   3   anio             40788 non-null  int64  
#   4   idh_tipo         40788 non-null  object 
#   5   valor            38647 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | idh_tipo                             |    valor |
#  |---:|:------------------|:------------------|:------------|-------:|:-------------------------------------|---------:|
#  |  0 | AFG               | Afganistán        | Afghanistan |   1990 | IDH años esperados de escolarización | 0.163137 |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   idh_tipo         198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | idh_tipo                             |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh_tipo': 'categoria'})
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   categoria        198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | categoria                            |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  