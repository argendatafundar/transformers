from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'letra_desc_abrev': 'categoria', 'porc_mujeres': 'valor'}),
	drop_col(col=['letra'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
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
#   3   porc_mujeres      390 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   porc_mujeres |
#  |---:|-------:|:--------|:-------------------|---------------:|
#  |  0 |   1996 | A       | Agro               |      0.0918044 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria', 'porc_mujeres': 'valor'})
#  RangeIndex: 390 entries, 0 to 389
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       390 non-null    int64  
#   1   letra      390 non-null    object 
#   2   categoria  390 non-null    object 
#   3   valor      390 non-null    float64
#  
#  |    |   anio | letra   | categoria   |     valor |
#  |---:|-------:|:--------|:------------|----------:|
#  |  0 |   1996 | A       | Agro        | 0.0918044 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra'], axis=1)
#  RangeIndex: 390 entries, 0 to 389
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       390 non-null    int64  
#   1   categoria  390 non-null    object 
#   2   valor      390 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.18044 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 390 entries, 0 to 389
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       390 non-null    int64  
#   1   categoria  390 non-null    object 
#   2   valor      390 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.18044 |
#  
#  ------------------------------
#  