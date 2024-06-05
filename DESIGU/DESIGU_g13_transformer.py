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
rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 111 entries, 0 to 110
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       111 non-null    int64  
#   1   variable  111 non-null    object 
#   2   valor     111 non-null    float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1986 | quintil1   |    6.74 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 111 entries, 0 to 110
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       111 non-null    int64  
#   1   indicador  111 non-null    object 
#   2   valor      111 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | quintil1    |    6.74 |
#  
#  ------------------------------
#  