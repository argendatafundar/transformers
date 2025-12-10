from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def custom_string_funcion(df:DataFrame): 
    df['categoria2'] = df['categoria'].str.removeprefix(prefix="Complejo ").apply(lambda x: x[0].upper() + x[1:])
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	custom_string_funcion(),
	sort_values(how='descending', by='exportaciones_en_usd_mill')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 885 entries, 0 to 884
#  Data columns (total 4 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  885 non-null    object 
#   1   anio                       885 non-null    int64  
#   2   exportaciones_en_usd_mill  885 non-null    float64
#   3   tipo                       885 non-null    object 
#  
#  |    | categoria             |   anio |   exportaciones_en_usd_mill | tipo   |
#  |---:|:----------------------|-------:|----------------------------:|:-------|
#  |  0 | Complejos oleaginosos |   2006 |                        9770 | Bienes |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 50 entries, 553 to 884
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |     | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |----:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  | 553 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  
#  custom_string_funcion()
#  Index: 50 entries, 553 to 884
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |     | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |----:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  | 553 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='exportaciones_en_usd_mill')
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |    | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |---:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  |  0 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  