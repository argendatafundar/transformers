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
def custom_string_funcion(df: DataFrame):
    df['industria2'] = (
        df['industria']
        .str.replace(r"^\d\. ", "", regex=True)
    )
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='agregado', curr_value='No turístico', new_value='No turística', mapping=None),
	replace_value(col='industria', curr_value='No turístico', new_value='No turística', mapping=None),
	custom_string_funcion()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 |
#  
#  ------------------------------
#  
#  replace_value(col='agregado', curr_value='No turístico', new_value='No turística', mapping=None)
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 |
#  
#  ------------------------------
#  
#  replace_value(col='industria', curr_value='No turístico', new_value='No turística', mapping=None)
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#   6   industria2         6 non-null      object 
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo | industria2                  |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|:----------------------------|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 | Alojamiento para visitantes |
#  
#  ------------------------------
#  
#  custom_string_funcion()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               6 non-null      int64  
#   1   agregado           6 non-null      object 
#   2   industria          6 non-null      object 
#   3   empleo_total       6 non-null      float64
#   4   prop               6 non-null      float64
#   5   prop_intraturismo  5 non-null      float64
#   6   industria2         6 non-null      object 
#  
#  |    |   anio | agregado   | industria                      |   empleo_total |     prop |   prop_intraturismo | industria2                  |
#  |---:|-------:|:-----------|:-------------------------------|---------------:|---------:|--------------------:|:----------------------------|
#  |  0 |   2022 | Turística  | 1. Alojamiento para visitantes |        92.3066 | 0.422351 |             7.70901 | Alojamiento para visitantes |
#  
#  ------------------------------
#  