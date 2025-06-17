from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_values(df: DataFrame, col: str, mapper: dict) -> DataFrame:
    df[col] = df[col].replace(mapper)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador'),
	replace_values(col='indicador', mapper={'pib_corriente': 'Precios corrientes', 'pib_constante': 'Precios constantes'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13650 entries, 0 to 13649
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           13650 non-null  object 
#   1   anio           13650 non-null  int64  
#   2   pib_corriente  13650 non-null  float64
#   3   pib_constante  13650 non-null  float64
#  
#  |    | iso3   |   anio |   pib_corriente |   pib_constante |
#  |---:|:-------|-------:|----------------:|----------------:|
#  |  0 | AFE    |   2023 |     1.24547e+12 |     1.06449e+12 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 62 entries, 2981 to 3042
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           62 non-null     object 
#   1   anio           62 non-null     int64  
#   2   pib_corriente  62 non-null     float64
#   3   pib_constante  62 non-null     float64
#  
#  |      | iso3   |   anio |   pib_corriente |   pib_constante |
#  |-----:|:-------|-------:|----------------:|----------------:|
#  | 2981 | ARG    |   2023 |     6.46075e+11 |      5.8896e+11 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 62 entries, 2981 to 3042
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           62 non-null     int64  
#   1   pib_corriente  62 non-null     float64
#   2   pib_constante  62 non-null     float64
#  
#  |      |   anio |   pib_corriente |   pib_constante |
#  |-----:|-------:|----------------:|----------------:|
#  | 2981 |   2023 |     6.46075e+11 |      5.8896e+11 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       124 non-null    int64  
#   1   indicador  124 non-null    object 
#   2   valor      124 non-null    float64
#  
#  |    |   anio | indicador          |       valor |
#  |---:|-------:|:-------------------|------------:|
#  |  0 |   2023 | Precios corrientes | 6.46075e+11 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', mapper={'pib_corriente': 'Precios corrientes', 'pib_constante': 'Precios constantes'})
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       124 non-null    int64  
#   1   indicador  124 non-null    object 
#   2   valor      124 non-null    float64
#  
#  |    |   anio | indicador          |       valor |
#  |---:|-------:|:-------------------|------------:|
#  |  0 |   2023 | Precios corrientes | 6.46075e+11 |
#  
#  ------------------------------
#  