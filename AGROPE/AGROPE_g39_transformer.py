from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='gran_rubro', curr_value='Manufacturas de origen agropecuario (MOA)', new_value='MOA'),
	replace_value(col='gran_rubro', curr_value='Productos primarios (PP)', new_value='Productos Primarios'),
	query(condition="division == 'Agroindustrial'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 47 entries, 0 to 46
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   division    47 non-null     object 
#   1   gran_rubro  47 non-null     object 
#   2   rubro       47 non-null     object 
#   3   anio        47 non-null     int64  
#   4   expo        47 non-null     float64
#   5   share       47 non-null     float64
#  
#  |    | division       | gran_rubro               | rubro          |   anio |    expo |      share |
#  |---:|:---------------|:-------------------------|:---------------|-------:|--------:|-----------:|
#  |  0 | Agroindustrial | Productos primarios (PP) | Animales vivos |   2024 | 25.6453 | 0.00032169 |
#  
#  ------------------------------
#  
#  replace_value(col='gran_rubro', curr_value='Manufacturas de origen agropecuario (MOA)', new_value='MOA')
#  RangeIndex: 47 entries, 0 to 46
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   division    47 non-null     object 
#   1   gran_rubro  47 non-null     object 
#   2   rubro       47 non-null     object 
#   3   anio        47 non-null     int64  
#   4   expo        47 non-null     float64
#   5   share       47 non-null     float64
#  
#  |    | division       | gran_rubro               | rubro          |   anio |    expo |      share |
#  |---:|:---------------|:-------------------------|:---------------|-------:|--------:|-----------:|
#  |  0 | Agroindustrial | Productos primarios (PP) | Animales vivos |   2024 | 25.6453 | 0.00032169 |
#  
#  ------------------------------
#  
#  replace_value(col='gran_rubro', curr_value='Productos primarios (PP)', new_value='Productos Primarios')
#  RangeIndex: 47 entries, 0 to 46
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   division    47 non-null     object 
#   1   gran_rubro  47 non-null     object 
#   2   rubro       47 non-null     object 
#   3   anio        47 non-null     int64  
#   4   expo        47 non-null     float64
#   5   share       47 non-null     float64
#  
#  |    | division       | gran_rubro          | rubro          |   anio |    expo |      share |
#  |---:|:---------------|:--------------------|:---------------|-------:|--------:|-----------:|
#  |  0 | Agroindustrial | Productos Primarios | Animales vivos |   2024 | 25.6453 | 0.00032169 |
#  
#  ------------------------------
#  
#  query(condition="division == 'Agroindustrial'")
#  Index: 27 entries, 0 to 26
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   division    27 non-null     object 
#   1   gran_rubro  27 non-null     object 
#   2   rubro       27 non-null     object 
#   3   anio        27 non-null     int64  
#   4   expo        27 non-null     float64
#   5   share       27 non-null     float64
#  
#  |    | division       | gran_rubro          | rubro          |   anio |    expo |      share |
#  |---:|:---------------|:--------------------|:---------------|-------:|--------:|-----------:|
#  |  0 | Agroindustrial | Productos Primarios | Animales vivos |   2024 | 25.6453 | 0.00032169 |
#  
#  ------------------------------
#  