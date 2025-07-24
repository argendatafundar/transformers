from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'sector': 'indicador'}),
	replace_value(col='indicador', curr_value='Servicios de inversión y desarrollo', new_value='Servicios de investigación y desarrollo'),
	rename_cols(map={'prop': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'indicador'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   prop    24 non-null     float64
#  
#  |    | sector                                                                                      |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   prop       24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Servicios de inversión y desarrollo', new_value='Servicios de investigación y desarrollo')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   prop       24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop': 'valor'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   valor      24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |   valor |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|--------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 2.79136 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   valor      24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |   valor |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|--------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 2.79136 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  24 non-null     object 
#   1   anio       24 non-null     int64  
#   2   valor      24 non-null     float64
#  
#  |    | indicador                                                                                   |   anio |   valor |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|--------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 2.79136 |
#  
#  ------------------------------
#  