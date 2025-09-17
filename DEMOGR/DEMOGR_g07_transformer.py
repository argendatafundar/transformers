from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity(),
	replace_value(col='fuente', curr_value='Dirección de Estadísticas e Información en Salud (DEIS)', new_value='DEIS', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 78 entries, 0 to 77
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    78 non-null     int64  
#   1   tgf     78 non-null     float64
#   2   fuente  78 non-null     object 
#  
#  |    |   anio |   tgf | fuente   |
#  |---:|-------:|------:|:---------|
#  |  0 |   1869 |   6.8 | INDEC    |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 78 entries, 0 to 77
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    78 non-null     int64  
#   1   tgf     78 non-null     float64
#   2   fuente  78 non-null     object 
#  
#  |    |   anio |   tgf | fuente   |
#  |---:|-------:|------:|:---------|
#  |  0 |   1869 |   6.8 | INDEC    |
#  
#  ------------------------------
#  
#  replace_value(col='fuente', curr_value='Dirección de Estadísticas e Información en Salud (DEIS)', new_value='DEIS', mapping=None)
#  RangeIndex: 78 entries, 0 to 77
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    78 non-null     int64  
#   1   tgf     78 non-null     float64
#   2   fuente  78 non-null     object 
#  
#  |    |   anio |   tgf | fuente   |
#  |---:|-------:|------:|:---------|
#  |  0 |   1869 |   6.8 | INDEC    |
#  
#  ------------------------------
#  