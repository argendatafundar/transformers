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
	replace_value(col='circa', curr_value=None, new_value=None, mapping={2000: 'circa_2000', 2021: 'circa_2021'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  22 non-null     object 
#   1   geonombreFundar  22 non-null     object 
#   2   anio_circa       22 non-null     int64  
#   3   brecha           22 non-null     float64
#   4   circa            22 non-null     int64  
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio_circa |   brecha |   circa |
#  |---:|:------------------|:------------------|-------------:|---------:|--------:|
#  |  0 | ARG               | Argentina         |         2000 |  13.9852 |    2000 |
#  
#  ------------------------------
#  
#  replace_value(col='circa', curr_value=None, new_value=None, mapping={2000: 'circa_2000', 2021: 'circa_2021'})
#  RangeIndex: 22 entries, 0 to 21
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  22 non-null     object 
#   1   geonombreFundar  22 non-null     object 
#   2   anio_circa       22 non-null     object 
#   3   brecha           22 non-null     float64
#   4   circa            22 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   | anio_circa   |   brecha | circa      |
#  |---:|:------------------|:------------------|:-------------|---------:|:-----------|
#  |  0 | ARG               | Argentina         | circa_2000   |  13.9852 | circa_2000 |
#  
#  ------------------------------
#  