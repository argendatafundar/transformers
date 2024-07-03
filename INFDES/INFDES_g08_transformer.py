from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'apertura_sexo': 'indicador'})
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
#  rename_cols(map={'apertura_sexo': 'indicador'})
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   indicador  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1986 | Mujer       | 0.299352 |
#  
#  ------------------------------
#  