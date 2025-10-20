from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict, default = None)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, default))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	map_categoria(curr_col='letra', new_col='grupo', mapper={'A_B': 'Primario', 'C': 'Primario', 'F': 'Resto de bienes', 'E': 'Resto de bienes', 'D': 'Manufacturas'}, default='Servicios'),
	drop_col(col=['anio', 'letra'], axis=1),
	pivot_longer(id_cols=['letra_desc', 'grupo'], names_to_col='variable', values_to_col='valor'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 294 entries, 0 to 293
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         294 non-null    int64  
#   1   letra        294 non-null    object 
#   2   letra_desc   294 non-null    object 
#   3   prop_empleo  294 non-null    float64
#   4   prop_vab     294 non-null    float64
#  
#  |    |   anio | letra   | letra_desc   |   prop_empleo |   prop_vab |
#  |---:|-------:|:--------|:-------------|--------------:|-----------:|
#  |  0 |   2004 | A_B     | Agro y pesca |     0.0836072 |  0.0983632 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 14 entries, 280 to 293
#  Data columns (total 6 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         14 non-null     int64  
#   1   letra        14 non-null     object 
#   2   letra_desc   14 non-null     object 
#   3   prop_empleo  14 non-null     float64
#   4   prop_vab     14 non-null     float64
#   5   grupo        14 non-null     object 
#  
#  |     |   anio | letra   | letra_desc   |   prop_empleo |   prop_vab | grupo    |
#  |----:|-------:|:--------|:-------------|--------------:|-----------:|:---------|
#  | 280 |   2024 | A_B     | Agro y pesca |     0.0644079 |  0.0717834 | Primario |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='letra', new_col='grupo', mapper={'A_B': 'Primario', 'C': 'Primario', 'F': 'Resto de bienes', 'E': 'Resto de bienes', 'D': 'Manufacturas'}, default='Servicios')
#  Index: 14 entries, 280 to 293
#  Data columns (total 6 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         14 non-null     int64  
#   1   letra        14 non-null     object 
#   2   letra_desc   14 non-null     object 
#   3   prop_empleo  14 non-null     float64
#   4   prop_vab     14 non-null     float64
#   5   grupo        14 non-null     object 
#  
#  |     |   anio | letra   | letra_desc   |   prop_empleo |   prop_vab | grupo    |
#  |----:|-------:|:--------|:-------------|--------------:|-----------:|:---------|
#  | 280 |   2024 | A_B     | Agro y pesca |     0.0644079 |  0.0717834 | Primario |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'letra'], axis=1)
#  Index: 14 entries, 280 to 293
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   letra_desc   14 non-null     object 
#   1   prop_empleo  14 non-null     float64
#   2   prop_vab     14 non-null     float64
#   3   grupo        14 non-null     object 
#  
#  |     | letra_desc   |   prop_empleo |   prop_vab | grupo    |
#  |----:|:-------------|--------------:|-----------:|:---------|
#  | 280 | Agro y pesca |     0.0644079 |  0.0717834 | Primario |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['letra_desc', 'grupo'], names_to_col='variable', values_to_col='valor')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   letra_desc  28 non-null     object 
#   1   grupo       28 non-null     object 
#   2   variable    28 non-null     object 
#   3   valor       28 non-null     float64
#  
#  |    | letra_desc   | grupo    | variable    |   valor |
#  |---:|:-------------|:---------|:------------|--------:|
#  |  0 | Agro y pesca | Primario | prop_empleo | 6.44079 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   letra_desc  28 non-null     object 
#   1   grupo       28 non-null     object 
#   2   variable    28 non-null     object 
#   3   valor       28 non-null     float64
#  
#  |    | letra_desc   | grupo    | variable    |   valor |
#  |---:|:-------------|:---------|:------------|--------:|
#  |  0 | Agro y pesca | Primario | prop_empleo | 6.44079 |
#  
#  ------------------------------
#  