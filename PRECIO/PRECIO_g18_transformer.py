from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def imput_na_by_condition(df:DataFrame, bool_mask:list[bool], col:str):
    from numpy import nan
    df.loc[bool_mask,col] = nan
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	imput_na_by_condition(bool_mask=0     False
1     False
2     False
3     False
4     False
      ...  
84    False
85    False
86    False
87    False
88    False
Name: tasa_inflacion, Length: 89, dtype: bool, col='tasa_inflacion')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            89 non-null     int64  
#   1   tasa_inflacion  89 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  
#  imput_na_by_condition(bool_mask=0     False
#  1     False
#  2     False
#  3     False
#  4     False
#        ...  
#  84    False
#  85    False
#  86    False
#  87    False
#  88    False
#  Name: tasa_inflacion, Length: 89, dtype: bool, col='tasa_inflacion')
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            89 non-null     int64  
#   1   tasa_inflacion  81 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |
#  |---:|-------:|-----------------:|
#  |  0 |   1935 |          14.0881 |
#  
#  ------------------------------
#  