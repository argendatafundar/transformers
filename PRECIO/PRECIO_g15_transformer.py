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
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             778 non-null    int64  
#   1   rubro            778 non-null    object 
#   2   precio_relativo  778 non-null    float64
#  
#  |    |   anio | rubro               |   precio_relativo |
#  |---:|-------:|:--------------------|------------------:|
#  |  0 |   2006 | Alimentos y bebidas |           97.3951 |
#  
#  ------------------------------
#  
#  rename_columns(rubro='grupo', precio_relativo='valor')
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    778 non-null    int64  
#   1   grupo   778 non-null    object 
#   2   valor   778 non-null    float64
#  
#  |    |   anio | grupo               |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2006 | Alimentos y bebidas | 97.3951 |
#  
#  ------------------------------
#  