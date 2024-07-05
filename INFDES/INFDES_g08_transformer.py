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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'apertura_sexo': 'categoria'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           105 non-null    int64  
#   1   apertura_sexo  105 non-null    object 
#   2   valor          105 non-null    float64
#  
#  |    |   anio | apertura_sexo   |    valor |
#  |---:|-------:|:----------------|---------:|
#  |  0 |   1986 | Mujer           | 0.299352 |
#  
#  ------------------------------
#  
#  rename_cols(map={'apertura_sexo': 'categoria'})
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Mujer       | 29.9352 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Mujer       | 29.9352 |
#  
#  ------------------------------
#  