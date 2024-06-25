from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == 2019 & productividad_tipo == "Productividad por ocupado"'),
	rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col=['productividad_tipo', 'anio'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 25620 entries, 0 to 25619
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                25620 non-null  object 
#   1   anio                25620 non-null  int64  
#   2   productividad_tipo  25620 non-null  object 
#   3   valor               13021 non-null  float64
#  
#  |    | iso3   |   anio | productividad_tipo     |   valor |
#  |---:|:-------|-------:|:-----------------------|--------:|
#  |  0 | ABW    |   1950 | Productividad por hora |     nan |
#  
#  ------------------------------
#  
#  query(condition='anio == 2019 & productividad_tipo == "Productividad por ocupado"')
#  Index: 183 entries, 139 to 25619
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                183 non-null    object 
#   1   anio                183 non-null    int64  
#   2   productividad_tipo  183 non-null    object 
#   3   valor               177 non-null    float64
#  
#  |     | iso3   |   anio | productividad_tipo        |   valor |
#  |----:|:-------|-------:|:--------------------------|--------:|
#  | 139 | ABW    |   2019 | Productividad por ocupado | 64468.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  Index: 183 entries, 139 to 25619
#  Data columns (total 4 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigo           183 non-null    object 
#   1   anio                183 non-null    int64  
#   2   productividad_tipo  183 non-null    object 
#   3   valor               177 non-null    float64
#  
#  |     | geocodigo   |   anio | productividad_tipo        |   valor |
#  |----:|:------------|-------:|:--------------------------|--------:|
#  | 139 | ABW         |   2019 | Productividad por ocupado | 64468.5 |
#  
#  ------------------------------
#  
#  drop_col(col=['productividad_tipo', 'anio'], axis=1)
#  Index: 183 entries, 139 to 25619
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  183 non-null    object 
#   1   valor      177 non-null    float64
#  
#  |     | geocodigo   |   valor |
#  |----:|:------------|--------:|
#  | 139 | ABW         | 64468.5 |
#  
#  ------------------------------
#  