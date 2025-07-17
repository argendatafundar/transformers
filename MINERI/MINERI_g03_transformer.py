from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'sector': 'indicador', 'exportaciones_sector_perc': 'valor'}),
	query(condition="indicador != 'otros'"),
	replace_value(col='indicador', curr_value='no_metaliferos', new_value='No metalíferos'),
	replace_value(col='indicador', curr_value='metaliferos', new_value='Metalíferos'),
	replace_value(col='indicador', curr_value='piedras_preciosas_semipreciosas', new_value='Piedras preciosas y semipreciosas'),
	replace_value(col='indicador', curr_value='rocas_de_aplicacion', new_value='Rocas de aplicación'),
	ordenar_categorica(col1='indicador', order1=['Metalíferos', 'No metalíferos', 'Rocas de aplicación', 'Piedras preciosas y semipreciosas']),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       140 non-null    int64  
#   1   sector                     140 non-null    object 
#   2   exportaciones_sector_perc  140 non-null    float64
#  
#  |    |   anio | sector      |   exportaciones_sector_perc |
#  |---:|-------:|:------------|----------------------------:|
#  |  0 |   1994 | metaliferos |                    0.170485 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'exportaciones_sector_perc': 'valor'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       140 non-null    int64  
#   1   indicador  140 non-null    object 
#   2   valor      140 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | metaliferos | 0.170485 |
#  
#  ------------------------------
#  
#  query(condition="indicador != 'otros'")
#  Index: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | metaliferos | 0.170485 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='no_metaliferos', new_value='No metalíferos')
#  Index: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | metaliferos | 0.170485 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='metaliferos', new_value='Metalíferos')
#  Index: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | Metalíferos | 0.170485 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='piedras_preciosas_semipreciosas', new_value='Piedras preciosas y semipreciosas')
#  Index: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | Metalíferos | 0.170485 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='rocas_de_aplicacion', new_value='Rocas de aplicación')
#  Index: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       116 non-null    int64   
#   1   indicador  116 non-null    category
#   2   valor      116 non-null    float64 
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | Metalíferos | 0.170485 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Metalíferos', 'No metalíferos', 'Rocas de aplicación', 'Piedras preciosas y semipreciosas'])
#  Index: 116 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       116 non-null    int64   
#   1   indicador  116 non-null    category
#   2   valor      116 non-null    float64 
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | Metalíferos | 0.170485 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       116 non-null    int64   
#   1   indicador  116 non-null    category
#   2   valor      116 non-null    float64 
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | Metalíferos | 0.170485 |
#  
#  ------------------------------
#  