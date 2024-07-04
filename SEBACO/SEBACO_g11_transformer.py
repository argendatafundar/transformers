from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
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
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'categoria', 'estado_ocupacional': 'indicador', 'prop_ocupados': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	resub(col='categoria', pattern='a.', replace=''),
	resub(col='categoria', pattern='b.', replace=''),
	resub(col='categoria', pattern='c.', replace=''),
	resub(col='categoria', pattern='d.', replace=''),
	resub(col='categoria', pattern='e.', replace=''),
	sort_values(how='ascending', by=['categoria', 'indicador'])
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
#   2   valor      15 non-null     float64
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
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
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
#   2   valor      15 non-null     float64
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria', 'indicador'])
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
#   2   valor      15 non-null     float64
#  
#  |    | categoria                                         | indicador                |   valor |
#  |---:|:--------------------------------------------------|:-------------------------|--------:|
#  |  0 | Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 14.6243 |
#  
#  ------------------------------
#  