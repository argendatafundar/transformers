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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='region', curr_value='América central', new_value='América Central', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 |
#  
#  ------------------------------
#  
#  replace_value(col='region', curr_value='América central', new_value='América Central', mapping=None)
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 |
#  
#  ------------------------------
#  