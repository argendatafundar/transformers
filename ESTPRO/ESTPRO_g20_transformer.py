from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio == anio.max()'),
	rename_cols(map={'escala': 'valor', 'letra_desc_abrev': 'categoria'}),
	drop_col(col=['anio', 'letra'], axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 390 entries, 0 to 389
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              390 non-null    int64  
#   1   letra             390 non-null    object 
#   2   letra_desc_abrev  390 non-null    object 
#   3   escala            390 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   escala |
#  |---:|-------:|:--------|:-------------------|---------:|
#  |  0 |   1996 | A       | Agro               |  4.12243 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 15 entries, 375 to 389
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              15 non-null     int64  
#   1   letra             15 non-null     object 
#   2   letra_desc_abrev  15 non-null     object 
#   3   escala            15 non-null     float64
#  
#  |     |   anio | letra   | letra_desc_abrev   |   escala |
#  |----:|-------:|:--------|:-------------------|---------:|
#  | 375 |   2021 | A       | Agro               |  5.91369 |
#  
#  ------------------------------
#  
#  rename_cols(map={'escala': 'valor', 'letra_desc_abrev': 'categoria'})
#  Index: 15 entries, 375 to 389
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       15 non-null     int64  
#   1   letra      15 non-null     object 
#   2   categoria  15 non-null     object 
#   3   valor      15 non-null     float64
#  
#  |     |   anio | letra   | categoria   |   valor |
#  |----:|-------:|:--------|:------------|--------:|
#  | 375 |   2021 | A       | Agro        | 5.91369 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'letra'], axis=1)
#  Index: 15 entries, 375 to 389
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   valor      15 non-null     float64
#  
#  |     | categoria   |   valor |
#  |----:|:------------|--------:|
#  | 375 | Agro        | 5.91369 |
#  
#  ------------------------------
#  