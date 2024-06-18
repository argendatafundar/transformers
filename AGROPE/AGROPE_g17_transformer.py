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
rename_cols(map={'tipo_carne': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   tipo_carne  183 non-null    object 
#   1   anio        183 non-null    int64  
#   2   valor       183 non-null    float64
#  
#  |    | tipo_carne   |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_carne': 'indicador'})
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  183 non-null    object 
#   1   anio       183 non-null    int64  
#   2   valor      183 non-null    float64
#  
#  |    | indicador    |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  