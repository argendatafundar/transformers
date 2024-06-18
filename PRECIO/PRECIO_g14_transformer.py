from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(rubro='grupo', precio_relativo='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             748 non-null    int64  
#   1   codigo           748 non-null    object 
#   2   nivel            748 non-null    int64  
#   3   rubro            748 non-null    object 
#   4   precio_relativo  748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | rubro                              |   precio_relativo |
#  |---:|-------:|---------:|--------:|:-----------------------------------|------------------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |               100 |
#  
#  ------------------------------
#  
#  rename_columns(rubro='grupo', precio_relativo='valor')
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    748 non-null    int64  
#   1   codigo  748 non-null    object 
#   2   nivel   748 non-null    int64  
#   3   grupo   748 non-null    object 
#   4   valor   748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | grupo                              |   valor |
#  |---:|-------:|---------:|--------:|:-----------------------------------|--------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |     100 |
#  
#  ------------------------------
#  