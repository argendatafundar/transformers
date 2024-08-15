from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def resub(df: DataFrame, col: str, pattern: str, replace: str):
    # replace regex pattern in column
    
    df[col] = df[col].str.replace(pattern, replace)
    return df

@transformer.convert
def resub(df: DataFrame, col: str, pattern: str, replace: str):
    # replace regex pattern in column
    
    df[col] = df[col].str.replace(pattern, replace)
    return df

@transformer.convert
def resub(df: DataFrame, col: str, pattern: str, replace: str):
    # replace regex pattern in column
    
    df[col] = df[col].str.replace(pattern, replace)
    return df

@transformer.convert
def resub(df: DataFrame, col: str, pattern: str, replace: str):
    # replace regex pattern in column
    
    df[col] = df[col].str.replace(pattern, replace)
    return df

@transformer.convert
def resub(df: DataFrame, col: str, pattern: str, replace: str):
    # replace regex pattern in column
    
    df[col] = df[col].str.replace(pattern, replace)
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'categoria', 'estado_ocupacional': 'indicador', 'prop_ocupados': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	resub(col='categoria', pattern='a.', replace=''),
	resub(col='categoria', pattern='b.', replace=''),
	resub(col='categoria', pattern='c.', replace=''),
	resub(col='categoria', pattern='d.', replace=''),
	resub(col='categoria', pattern='e.', replace=''),
	ordenar_dos_columnas(col1='indicador', order1=['Asalariado registrado', 'Asalariado no registrado', 'No asalariado'], col2='categoria', order2=[' Actividades profesionales, cientificas y técnicas', ' Sector privado', ' SBC', ' Total economía', ' SSI'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   sector              15 non-null     object 
#   1   estado_ocupacional  15 non-null     object 
#   2   prop_ocupados       15 non-null     float64
#  
#  |    | sector                                               | estado_ocupacional       |   prop_ocupados |
#  |---:|:-----------------------------------------------------|:-------------------------|----------------:|
#  |  0 | a. Actividades profesionales, cientificas y técnicas | Asalariado no registrado |        0.146243 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'categoria', 'estado_ocupacional': 'indicador', 'prop_ocupados': 'valor'})
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  resub(col='categoria', pattern='a.', replace='')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  resub(col='categoria', pattern='b.', replace='')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  resub(col='categoria', pattern='c.', replace='')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  resub(col='categoria', pattern='d.', replace='')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  resub(col='categoria', pattern='e.', replace='')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='indicador', order1=['Asalariado registrado', 'Asalariado no registrado', 'No asalariado'], col2='categoria', order2=[' Actividades profesionales, cientificas y técnicas', ' Sector privado', ' SBC', ' Total economía', ' SSI'])
#  Index: 15 entries, 1 to 5
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  15 non-null     category
#   1   indicador  15 non-null     category
#   2   valor      15 non-null     float64 
#  
#  |    | categoria                                         | indicador             |   valor |
#  |---:|:--------------------------------------------------|:----------------------|--------:|
#  |  1 | Actividades profesionales, cientificas y técnicas | Asalariado registrado |  32.478 |
#  
#  ------------------------------
#  