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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2022'),
	rename_cols(map={'indice_va_trab': 'valor', 'letra_desc_abrev': 'categoria'}),
	drop_col(col=['letra', 'anio', 'va_por_trabajador'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               128 non-null    int64  
#   1   letra              128 non-null    object 
#   2   letra_desc_abrev   128 non-null    object 
#   3   va_por_trabajador  128 non-null    float64
#   4   indice_va_trab     128 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   va_por_trabajador |   indice_va_trab |
#  |---:|-------:|:--------|:-------------------|--------------------:|-----------------:|
#  |  0 |   2016 | A       | Agro               |             370.038 |          106.623 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2022')
#  Index: 16 entries, 6 to 126
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               16 non-null     int64  
#   1   letra              16 non-null     object 
#   2   letra_desc_abrev   16 non-null     object 
#   3   va_por_trabajador  16 non-null     float64
#   4   indice_va_trab     16 non-null     float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   va_por_trabajador |   indice_va_trab |
#  |---:|-------:|:--------|:-------------------|--------------------:|-----------------:|
#  |  6 |   2022 | A       | Agro               |              3839.1 |          121.375 |
#  
#  ------------------------------
#  
#  rename_cols(map={'indice_va_trab': 'valor', 'letra_desc_abrev': 'categoria'})
#  Index: 16 entries, 6 to 126
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               16 non-null     int64  
#   1   letra              16 non-null     object 
#   2   categoria          16 non-null     object 
#   3   va_por_trabajador  16 non-null     float64
#   4   valor              16 non-null     float64
#  
#  |    |   anio | letra   | categoria   |   va_por_trabajador |   valor |
#  |---:|-------:|:--------|:------------|--------------------:|--------:|
#  |  6 |   2022 | A       | Agro        |              3839.1 | 121.375 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'anio', 'va_por_trabajador'], axis=1)
#  Index: 16 entries, 6 to 126
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  16 non-null     object 
#   1   valor      16 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  6 | Agro        | 121.375 |
#  
#  ------------------------------
#  