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
rename_cols(map={'   pais': 'categoria'}),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='BOL', new_value='Bolivia'),
	replace_value(col='categoria', curr_value='BRA', new_value='Brasil'),
	replace_value(col='categoria', curr_value='CHL', new_value='Chile'),
	replace_value(col='categoria', curr_value='COL', new_value='Colombia'),
	replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica'),
	replace_value(col='categoria', curr_value='DOM', new_value='Dominicana'),
	replace_value(col='categoria', curr_value='ECU', new_value='Ecuador'),
	replace_value(col='categoria', curr_value='GTM', new_value='Guatemala'),
	replace_value(col='categoria', curr_value='HND', new_value='Honduras'),
	replace_value(col='categoria', curr_value='MEX', new_value='México'),
	replace_value(col='categoria', curr_value='NIC', new_value='Nicaragua'),
	replace_value(col='categoria', curr_value='PAN', new_value='Panamá'),
	replace_value(col='categoria', curr_value='PER', new_value='Perú'),
	replace_value(col='categoria', curr_value='PRY', new_value='Paraguay'),
	replace_value(col='categoria', curr_value='SLV', new_value='El Salvador'),
	replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0      pais         17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    |    pais   |   salariohorario |
#  |---:|:----------|-----------------:|
#  |  0 | HND       |              2.7 |
#  
#  ------------------------------
#  
#  rename_cols(map={'   pais': 'categoria'})
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BOL', new_value='Bolivia')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BRA', new_value='Brasil')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHL', new_value='Chile')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='COL', new_value='Colombia')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DOM', new_value='Dominicana')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ECU', new_value='Ecuador')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GTM', new_value='Guatemala')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | HND         |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='HND', new_value='Honduras')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MEX', new_value='México')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='NIC', new_value='Nicaragua')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAN', new_value='Panamá')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PER', new_value='Perú')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRY', new_value='Paraguay')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLV', new_value='El Salvador')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   categoria       17 non-null     object 
#   1   salariohorario  17 non-null     float64
#  
#  |    | categoria   |   salariohorario |
#  |---:|:------------|-----------------:|
#  |  0 | Honduras    |              2.7 |
#  
#  ------------------------------
#  