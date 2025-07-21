from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def calculate_relative_percentages(df: DataFrame, group_col: str, value_col: str):
    totals = df.groupby(group_col)[value_col].transform('sum')
    df[value_col] = (df[value_col] / totals) * 100
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    import numpy as np
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'provincia': 'indicador', 'vab_min_provincial': 'valor'}),
	replace_values(col='indicador', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No_distribuido': 'MINERI_NO-DIST'}),
	replace_value(col='indicador', curr_value='AR-C', new_value='Ciudad Autónoma de Buenos Aires'),
	replace_value(col='indicador', curr_value='AR-B', new_value='Buenos Aires'),
	replace_value(col='indicador', curr_value='AR-K', new_value='Catamarca'),
	replace_value(col='indicador', curr_value='AR-X', new_value='Córdoba'),
	replace_value(col='indicador', curr_value='AR-W', new_value='Corrientes'),
	replace_value(col='indicador', curr_value='AR-H', new_value='Chaco'),
	replace_value(col='indicador', curr_value='AR-U', new_value='Chubut'),
	replace_value(col='indicador', curr_value='AR-E', new_value='Entre Ríos'),
	replace_value(col='indicador', curr_value='AR-P', new_value='Formosa'),
	replace_value(col='indicador', curr_value='AR-Y', new_value='Jujuy'),
	replace_value(col='indicador', curr_value='AR-L', new_value='La Pampa'),
	replace_value(col='indicador', curr_value='AR-F', new_value='La Rioja'),
	replace_value(col='indicador', curr_value='AR-M', new_value='Mendoza'),
	replace_value(col='indicador', curr_value='AR-N', new_value='Misiones'),
	replace_value(col='indicador', curr_value='AR-Q', new_value='Neuquén'),
	replace_value(col='indicador', curr_value='AR-R', new_value='Río Negro'),
	replace_value(col='indicador', curr_value='AR-A', new_value='Salta'),
	replace_value(col='indicador', curr_value='AR-J', new_value='San Juan'),
	replace_value(col='indicador', curr_value='AR-D', new_value='San Luis'),
	replace_value(col='indicador', curr_value='AR-Z', new_value='Santa Cruz'),
	replace_value(col='indicador', curr_value='AR-S', new_value='Santa Fe'),
	replace_value(col='indicador', curr_value='AR-G', new_value='Santiago del Estero'),
	replace_value(col='indicador', curr_value='AR-T', new_value='Tucumán'),
	replace_value(col='indicador', curr_value='AR-V', new_value='Tierra del Fuego'),
	replace_value(col='indicador', curr_value='MINERI_NO-DIST', new_value='No distribuído'),
	calculate_relative_percentages(group_col='anio', value_col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   provincia           475 non-null    object 
#   1   anio                475 non-null    int64  
#   2   vab_min_provincial  475 non-null    float64
#  
#  |    | provincia              |   anio |   vab_min_provincial |
#  |---:|:-----------------------|-------:|---------------------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 |              112.651 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'indicador', 'vab_min_provincial': 'valor'})
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador              |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'Ciudad_de_Buenos_Aires': 'AR-C', 'Buenos_Aires': 'AR-B', 'Catamarca': 'AR-K', 'Cordoba': 'AR-X', 'Corrientes': 'AR-W', 'Chaco': 'AR-H', 'Chubut': 'AR-U', 'Entre_Rios': 'AR-E', 'Formosa': 'AR-P', 'Jujuy': 'AR-Y', 'La_Pampa': 'AR-L', 'La_Rioja': 'AR-F', 'Mendoza': 'AR-M', 'Misiones': 'AR-N', 'Neuquen': 'AR-Q', 'Rio_Negro': 'AR-R', 'Salta': 'AR-A', 'San_Juan': 'AR-J', 'San_Luis': 'AR-D', 'Santa_Cruz': 'AR-Z', 'Santa_Fe': 'AR-S', 'Santiago_del_Estero': 'AR-G', 'Tucuman': 'AR-T', 'Tierra_del_Fuego': 'AR-V', 'No_distribuido': 'MINERI_NO-DIST'})
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-C', new_value='Ciudad Autónoma de Buenos Aires')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-B', new_value='Buenos Aires')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-K', new_value='Catamarca')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-X', new_value='Córdoba')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-W', new_value='Corrientes')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-H', new_value='Chaco')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-U', new_value='Chubut')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-E', new_value='Entre Ríos')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-P', new_value='Formosa')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-Y', new_value='Jujuy')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-L', new_value='La Pampa')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-F', new_value='La Rioja')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-M', new_value='Mendoza')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-N', new_value='Misiones')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-Q', new_value='Neuquén')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-R', new_value='Río Negro')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-A', new_value='Salta')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-J', new_value='San Juan')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-D', new_value='San Luis')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-Z', new_value='Santa Cruz')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-S', new_value='Santa Fe')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-G', new_value='Santiago del Estero')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-T', new_value='Tucumán')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AR-V', new_value='Tierra del Fuego')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='MINERI_NO-DIST', new_value='No distribuído')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 3.79645 |
#  
#  ------------------------------
#  
#  calculate_relative_percentages(group_col='anio', value_col='valor')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  475 non-null    object 
#   1   anio       475 non-null    int64  
#   2   valor      475 non-null    float64
#  
#  |    | indicador                       |   anio |   valor |
#  |---:|:--------------------------------|-------:|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires |   2004 | 3.79645 |
#  
#  ------------------------------
#  