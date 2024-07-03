from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2023'),
	drop_col(col='anio', axis=1),
	rename_cols(map={'rango_edad': 'categoria', 'sexo': 'indicador', 'tasa_desocupacion': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               64 non-null     int64  
#   1   rango_edad         64 non-null     int64  
#   2   rango_edad_desc    64 non-null     object 
#   3   sexo               64 non-null     object 
#   4   tasa_desocupacion  64 non-null     float64
#  
#  |    |   anio |   rango_edad | rango_edad_desc   | sexo    |   tasa_desocupacion |
#  |---:|-------:|-------------:|:------------------|:--------|--------------------:|
#  |  0 |   2016 |            1 | Hasta 30          | Mujeres |            0.193575 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2023')
#  Index: 8 entries, 56 to 63
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               8 non-null      int64  
#   1   rango_edad         8 non-null      int64  
#   2   rango_edad_desc    8 non-null      object 
#   3   sexo               8 non-null      object 
#   4   tasa_desocupacion  8 non-null      float64
#  
#  |    |   anio |   rango_edad | rango_edad_desc   | sexo    |   tasa_desocupacion |
#  |---:|-------:|-------------:|:------------------|:--------|--------------------:|
#  | 56 |   2023 |            1 | Hasta 30          | Mujeres |            0.124757 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 8 entries, 56 to 63
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   rango_edad         8 non-null      int64  
#   1   rango_edad_desc    8 non-null      object 
#   2   sexo               8 non-null      object 
#   3   tasa_desocupacion  8 non-null      float64
#  
#  |    |   rango_edad | rango_edad_desc   | sexo    |   tasa_desocupacion |
#  |---:|-------------:|:------------------|:--------|--------------------:|
#  | 56 |            1 | Hasta 30          | Mujeres |            0.124757 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rango_edad': 'categoria', 'sexo': 'indicador', 'tasa_desocupacion': 'valor'})
#  Index: 8 entries, 56 to 63
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   categoria        8 non-null      int64  
#   1   rango_edad_desc  8 non-null      object 
#   2   indicador        8 non-null      object 
#   3   valor            8 non-null      float64
#  
#  |    |   categoria | rango_edad_desc   | indicador   |    valor |
#  |---:|------------:|:------------------|:------------|---------:|
#  | 56 |           1 | Hasta 30          | Mujeres     | 0.124757 |
#  
#  ------------------------------
#  