from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'tipo_carne': 'indicador'}),
	replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos'),
	replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes'),
	replace_value(col='indicador', curr_value='aviar', new_value='Aviar'),
	replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina'),
	replace_value(col='indicador', curr_value='porcina', new_value='Porcina'),
	replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna'),
	query(condition="geocodigoFundar != 'F351'"),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Cuyo', 'NEA', 'NOA', 'Pampeana', 'Patagonia'], col2='anio', order2=None)
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
#  rename_cols(map={'tipo_carne': 'indicador'})
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
#  replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos')
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
#  replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes')
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
#  replace_value(col='indicador', curr_value='aviar', new_value='Aviar')
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
#  replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina')
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
#  replace_value(col='indicador', curr_value='porcina', new_value='Porcina')
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
#  replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna')
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
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  80 non-null     object  
#   1   geonombreFundar  80 non-null     category
#   2   anio             80 non-null     category
#   3   valor            80 non-null     float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Cuyo', 'NEA', 'NOA', 'Pampeana', 'Patagonia'], col2='anio', order2=None)
#  Index: 80 entries, 0 to 79
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  80 non-null     object  
#   1   geonombreFundar  80 non-null     category
#   2   anio             80 non-null     category
#   3   valor            80 non-null     float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|:------------------|-------:|------------:|
#  |  0 | AR-CUY            | Cuyo              |   2007 | 2.01333e+06 |
#  
#  ------------------------------
#  