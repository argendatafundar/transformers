from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def imput_na(df:DataFrame, bool_mask:list[bool], col:str, value:Any):
    df.loc[bool_mask,col] = value
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True], col='geonombreFundar', value='Argentina')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 197 entries, 0 to 196
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  189 non-null    object 
#   1   geonombreFundar  189 non-null    object 
#   2   anio             197 non-null    int64  
#   3   tasa_empleo      197 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |
#  |---:|:------------------|:------------------|-------:|--------------:|
#  |  0 | AR-C              | CABA              |   2016 |      0.513004 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 25 entries, 165 to 196
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  25 non-null     object 
#   2   anio             25 non-null     int64  
#   3   tasa_empleo      25 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|--------------:|
#  | 165 | AR-C              | CABA              |   2023 |      0.528291 |
#  
#  ------------------------------
#  
#  imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True], col='geonombreFundar', value='Argentina')
#  Index: 25 entries, 165 to 196
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  25 non-null     object 
#   2   anio             25 non-null     int64  
#   3   tasa_empleo      25 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |
#  |----:|:------------------|:------------------|-------:|--------------:|
#  | 165 | AR-C              | CABA              |   2023 |      0.528291 |
#  
#  ------------------------------
#  