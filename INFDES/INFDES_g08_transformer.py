from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=100),
	query(condition="apertura_sexo != 'Brecha'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           105 non-null    int64  
#   1   apertura_sexo  105 non-null    object 
#   2   valor          105 non-null    float64
#  
#  |    |   anio | apertura_sexo   |    valor |
#  |---:|-------:|:----------------|---------:|
#  |  0 |   1986 | Mujer           | 0.299352 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           105 non-null    int64  
#   1   apertura_sexo  105 non-null    object 
#   2   valor          105 non-null    float64
#  
#  |    |   anio | apertura_sexo   |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1986 | Mujer           | 29.9352 |
#  
#  ------------------------------
#  
#  query(condition="apertura_sexo != 'Brecha'")
#  Index: 70 entries, 0 to 103
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           70 non-null     int64  
#   1   apertura_sexo  70 non-null     object 
#   2   valor          70 non-null     float64
#  
#  |    |   anio | apertura_sexo   |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1986 | Mujer           | 29.9352 |
#  
#  ------------------------------
#  