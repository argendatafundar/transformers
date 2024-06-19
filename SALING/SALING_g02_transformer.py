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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'country_code': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'}),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='BOL', new_value='Bolivia'),
	replace_value(col='categoria', curr_value='BRA', new_value='Brasil'),
	replace_value(col='categoria', curr_value='CHL', new_value='Chile'),
	replace_value(col='categoria', curr_value='COL', new_value='Colombia'),
	replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica'),
	replace_value(col='categoria', curr_value='DOM', new_value='Dominicana'),
	replace_value(col='categoria', curr_value='ECU', new_value='Ecuador'),
	replace_value(col='categoria', curr_value='MEX', new_value='México'),
	replace_value(col='categoria', curr_value='PAN', new_value='Panamá'),
	replace_value(col='categoria', curr_value='PER', new_value='Perú'),
	replace_value(col='categoria', curr_value='PRY', new_value='Paraguay'),
	replace_value(col='categoria', curr_value='SLV', new_value='El Salvador'),
	replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   country_code   14 non-null     object 
#   1   year           14 non-null     int64  
#   2   ipcf_promedio  14 non-null     float64
#  
#  |    | country_code   |   year |   ipcf_promedio |
#  |---:|:---------------|-------:|----------------:|
#  |  0 | ARG            |   2022 |         695.306 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BOL', new_value='Bolivia')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BRA', new_value='Brasil')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHL', new_value='Chile')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='COL', new_value='Colombia')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DOM', new_value='Dominicana')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ECU', new_value='Ecuador')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MEX', new_value='México')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAN', new_value='Panamá')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PER', new_value='Perú')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRY', new_value='Paraguay')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLV', new_value='El Salvador')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  14 non-null     object 
#   1   anio       14 non-null     int64  
#   2   valor      14 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   2022 | 695.306 |
#  
#  ------------------------------
#  