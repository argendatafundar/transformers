from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict, default:str = None)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, default))
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def filtrar_por_max_anio(df, group_cols):
    idx = df.groupby(group_cols)['anio'].transform('max') == df['anio']
    return df[idx]
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geonombreFundar not in ['Kirguistán']"),
	filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado']),
	map_categoria(curr_col='geonombreFundar', new_col='color', mapper={'Argentina': 'Argentina', 'Suecia': 'Suecia', 'Noruega': 'Noruega'}, default='Otros')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    193 non-null    object 
#   1   geonombreFundar    193 non-null    object 
#   2   anio               193 non-null    int64  
#   3   estado             193 non-null    object 
#   4   satisfaccion_vida  193 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar not in ['Kirguistán']")
#  Index: 192 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    192 non-null    object 
#   1   geonombreFundar    192 non-null    object 
#   2   anio               192 non-null    int64  
#   3   estado             192 non-null    object 
#   4   satisfaccion_vida  192 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado'])
#  Index: 178 entries, 0 to 192
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    178 non-null    object 
#   1   geonombreFundar    178 non-null    object 
#   2   anio               178 non-null    int64  
#   3   estado             178 non-null    object 
#   4   satisfaccion_vida  178 non-null    float64
#   5   color              178 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida | color   |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|:--------|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 | Otros   |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='geonombreFundar', new_col='color', mapper={'Argentina': 'Argentina', 'Suecia': 'Suecia', 'Noruega': 'Noruega'}, default='Otros')
#  Index: 178 entries, 0 to 192
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    178 non-null    object 
#   1   geonombreFundar    178 non-null    object 
#   2   anio               178 non-null    int64  
#   3   estado             178 non-null    object 
#   4   satisfaccion_vida  178 non-null    float64
#   5   color              178 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida | color   |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|:--------|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 | Otros   |
#  
#  ------------------------------
#  