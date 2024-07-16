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
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_energia': 'categoria', 'mineral_critico': 'indicador', 'mineral_utilizado': 'valor'}),
	replace_value(col='indicador', curr_value='cobre', new_value='Cobre'),
	replace_value(col='indicador', curr_value='niquel', new_value='Níquel'),
	replace_value(col='indicador', curr_value='manganeso', new_value='Manganeso'),
	replace_value(col='indicador', curr_value='cobalto', new_value='Cobalto'),
	replace_value(col='indicador', curr_value='cromo', new_value='Cromo'),
	replace_value(col='indicador', curr_value='molibdeno', new_value='Molibdeno'),
	replace_value(col='indicador', curr_value='cinc', new_value='Cinc'),
	replace_value(col='indicador', curr_value='tierras_raras', new_value='Tierras raras'),
	replace_value(col='indicador', curr_value='silicio', new_value='Silicio'),
	replace_value(col='indicador', curr_value='otros', new_value='Otros'),
	replace_values(col='categoria', values={'eolica_offshore': 'Eólica offshore', 'eolica_onshore': 'Eólica onshore', 'solar_fotovoltaica': 'Solar fotovoltaica', 'nuclear': 'Nuclear', 'carbon': 'Carbon', 'gas_natural': 'Gas natural'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   tipo_energia       60 non-null     object 
#   1   mineral_critico    60 non-null     object 
#   2   mineral_utilizado  60 non-null     float64
#  
#  |    | tipo_energia    | mineral_critico   |   mineral_utilizado |
#  |---:|:----------------|:------------------|--------------------:|
#  |  0 | eolica_offshore | cobre             |                8000 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'categoria', 'mineral_critico': 'indicador', 'mineral_utilizado': 'valor'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='cobre', new_value='Cobre')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='niquel', new_value='Níquel')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='manganeso', new_value='Manganeso')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='cobalto', new_value='Cobalto')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='cromo', new_value='Cromo')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='molibdeno', new_value='Molibdeno')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='cinc', new_value='Cinc')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tierras_raras', new_value='Tierras raras')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='silicio', new_value='Silicio')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='otros', new_value='Otros')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | eolica_offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  
#  replace_values(col='categoria', values={'eolica_offshore': 'Eólica offshore', 'eolica_onshore': 'Eólica onshore', 'solar_fotovoltaica': 'Solar fotovoltaica', 'nuclear': 'Nuclear', 'carbon': 'Carbon', 'gas_natural': 'Gas natural'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    | categoria       | indicador   |   valor |
#  |---:|:----------------|:------------|--------:|
#  |  0 | Eólica offshore | Cobre       |    8000 |
#  
#  ------------------------------
#  