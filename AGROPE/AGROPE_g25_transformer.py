from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_una_columna(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	ordenar_una_columna(col1='geonombreFundar', order1=['Cuyo', 'NEA', 'NOA', 'Pampeana', 'Patagonia'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  80 non-null     object 
#   1   geonombreFundar  80 non-null     object 
#   2   anio             80 non-null     int64  
#   3   valor            80 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  
#  ordenar_una_columna(col1='geonombreFundar', order1=['Cuyo', 'NEA', 'NOA', 'Pampeana', 'Patagonia'])
#  Index: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  80 non-null     object  
#   1   geonombreFundar  80 non-null     category
#   2   anio             80 non-null     int64   
#   3   valor            80 non-null     float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  