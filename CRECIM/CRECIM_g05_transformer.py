from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
query(condition="iso3 == 'ARG'"),
	drop_col(col='pais_nombre', axis=1),
	drop_col(col='iso3', axis=1),
	drop_col(col='continente_fundar', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	wide_to_long(primary_keys=['anio'], value_name='valor', var_name='categoria'),
	mutiplicar_por_escalar(col='valor', k=1e-06),
	replace_value(col='categoria', curr_value='pib_corriente', new_value='PIB corriente'),
	replace_value(col='categoria', curr_value='pib_constante', new_value='PIB constante')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12657 entries, 0 to 12656
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               12657 non-null  object 
#   1   pais_nombre        12657 non-null  object 
#   2   continente_fundar  9941 non-null   object 
#   3   anio               12657 non-null  int64  
#   4   pib_corriente      12657 non-null  float64
#   5   pib_constante      12657 non-null  float64
#   6   nivel_agregacion   12657 non-null  object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   pib_corriente |   pib_constante | nivel_agregacion   |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|----------------:|----------------:|:-------------------|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1986 |       4.056e+08 |       9.826e+08 | pais               |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 61 entries, 413 to 473
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               61 non-null     object 
#   1   pais_nombre        61 non-null     object 
#   2   continente_fundar  61 non-null     object 
#   3   anio               61 non-null     int64  
#   4   pib_corriente      61 non-null     float64
#   5   pib_constante      61 non-null     float64
#   6   nivel_agregacion   61 non-null     object 
#  
#  |     | iso3   | pais_nombre   | continente_fundar   |   anio |   pib_corriente |   pib_constante | nivel_agregacion   |
#  |----:|:-------|:--------------|:--------------------|-------:|----------------:|----------------:|:-------------------|
#  | 413 | ARG    | Argentina     | América del Sur     |   1962 |       2.445e+10 |       1.576e+11 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  Index: 61 entries, 413 to 473
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               61 non-null     object 
#   1   continente_fundar  61 non-null     object 
#   2   anio               61 non-null     int64  
#   3   pib_corriente      61 non-null     float64
#   4   pib_constante      61 non-null     float64
#   5   nivel_agregacion   61 non-null     object 
#  
#  |     | iso3   | continente_fundar   |   anio |   pib_corriente |   pib_constante | nivel_agregacion   |
#  |----:|:-------|:--------------------|-------:|----------------:|----------------:|:-------------------|
#  | 413 | ARG    | América del Sur     |   1962 |       2.445e+10 |       1.576e+11 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 61 entries, 413 to 473
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  61 non-null     object 
#   1   anio               61 non-null     int64  
#   2   pib_corriente      61 non-null     float64
#   3   pib_constante      61 non-null     float64
#   4   nivel_agregacion   61 non-null     object 
#  
#  |     | continente_fundar   |   anio |   pib_corriente |   pib_constante | nivel_agregacion   |
#  |----:|:--------------------|-------:|----------------:|----------------:|:-------------------|
#  | 413 | América del Sur     |   1962 |       2.445e+10 |       1.576e+11 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  Index: 61 entries, 413 to 473
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              61 non-null     int64  
#   1   pib_corriente     61 non-null     float64
#   2   pib_constante     61 non-null     float64
#   3   nivel_agregacion  61 non-null     object 
#  
#  |     |   anio |   pib_corriente |   pib_constante | nivel_agregacion   |
#  |----:|-------:|----------------:|----------------:|:-------------------|
#  | 413 |   1962 |       2.445e+10 |       1.576e+11 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  Index: 61 entries, 413 to 473
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           61 non-null     int64  
#   1   pib_corriente  61 non-null     float64
#   2   pib_constante  61 non-null     float64
#  
#  |     |   anio |   pib_corriente |   pib_constante |
#  |----:|-------:|----------------:|----------------:|
#  | 413 |   1962 |       2.445e+10 |       1.576e+11 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio'], value_name='valor', var_name='categoria')
#  RangeIndex: 122 entries, 0 to 121
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       122 non-null    int64  
#   1   categoria  122 non-null    object 
#   2   valor      122 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1962 | pib_corriente |   24450 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 122 entries, 0 to 121
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       122 non-null    int64  
#   1   categoria  122 non-null    object 
#   2   valor      122 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1962 | pib_corriente |   24450 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='pib_corriente', new_value='PIB corriente')
#  RangeIndex: 122 entries, 0 to 121
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       122 non-null    int64  
#   1   categoria  122 non-null    object 
#   2   valor      122 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1962 | PIB corriente |   24450 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='pib_constante', new_value='PIB constante')
#  RangeIndex: 122 entries, 0 to 121
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       122 non-null    int64  
#   1   categoria  122 non-null    object 
#   2   valor      122 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1962 | PIB corriente |   24450 |
#  
#  ------------------------------
#  