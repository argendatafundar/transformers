from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
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
drop_col(col='prov_cod', axis=1),
	wide_to_long(primary_keys=['prov_desc'], value_name='valor', var_name='indicador'),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='tasa_empleo_18_65_mujeres', new_value='Tasa de empleo femenino'),
	replace_value(col='indicador', curr_value='prop_usa_lavarropas', new_value='Hogares con lavarropas'),
	replace_value(col='prov_desc', curr_value='CABA', new_value='AR-C'),
	replace_value(col='prov_desc', curr_value='Buenos Aires', new_value='AR-B'),
	replace_value(col='prov_desc', curr_value='Catamarca', new_value='AR-K'),
	replace_value(col='prov_desc', curr_value='Córdoba', new_value='AR-X'),
	replace_value(col='prov_desc', curr_value='Corrientes', new_value='AR-W'),
	replace_value(col='prov_desc', curr_value='Chaco', new_value='AR-H'),
	replace_value(col='prov_desc', curr_value='Chubut', new_value='AR-U'),
	replace_value(col='prov_desc', curr_value='Entre Ríos', new_value='AR-E'),
	replace_value(col='prov_desc', curr_value='Formosa', new_value='AR-P'),
	replace_value(col='prov_desc', curr_value='Jujuy', new_value='AR-Y'),
	replace_value(col='prov_desc', curr_value='La Pampa', new_value='AR-L'),
	replace_value(col='prov_desc', curr_value='La Rioja', new_value='AR-F'),
	replace_value(col='prov_desc', curr_value='Mendoza', new_value='AR-M'),
	replace_value(col='prov_desc', curr_value='Misiones', new_value='AR-N'),
	replace_value(col='prov_desc', curr_value='Neuquén', new_value='AR-Q'),
	replace_value(col='prov_desc', curr_value='Río Negro', new_value='AR-R'),
	replace_value(col='prov_desc', curr_value='Salta', new_value='AR-A'),
	replace_value(col='prov_desc', curr_value='San Juan', new_value='AR-J'),
	replace_value(col='prov_desc', curr_value='San Luis', new_value='AR-D'),
	replace_value(col='prov_desc', curr_value='Santa Cruz', new_value='AR-Z'),
	replace_value(col='prov_desc', curr_value='Santa Fe', new_value='AR-S'),
	replace_value(col='prov_desc', curr_value='Santiago del Estero', new_value='AR-G'),
	replace_value(col='prov_desc', curr_value='Tucumán', new_value='AR-T'),
	replace_value(col='prov_desc', curr_value='Tierra del Fuego', new_value='AR-V')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   prov_cod                   24 non-null     int64  
#   1   prov_desc                  24 non-null     object 
#   2   tasa_empleo_18_65_mujeres  24 non-null     float64
#   3   prop_usa_lavarropas        24 non-null     float64
#  
#  |    |   prov_cod | prov_desc   |   tasa_empleo_18_65_mujeres |   prop_usa_lavarropas |
#  |---:|-----------:|:------------|----------------------------:|----------------------:|
#  |  0 |          2 | CABA        |                     0.75518 |              0.766963 |
#  
#  ------------------------------
#  
#  drop_col(col='prov_cod', axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   prov_desc                  24 non-null     object 
#   1   tasa_empleo_18_65_mujeres  24 non-null     float64
#   2   prop_usa_lavarropas        24 non-null     float64
#  
#  |    | prov_desc   |   tasa_empleo_18_65_mujeres |   prop_usa_lavarropas |
#  |---:|:------------|----------------------------:|----------------------:|
#  |  0 | CABA        |                     0.75518 |              0.766963 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['prov_desc'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador                 |   valor |
#  |---:|:------------|:--------------------------|--------:|
#  |  0 | CABA        | tasa_empleo_18_65_mujeres |  75.518 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador                 |   valor |
#  |---:|:------------|:--------------------------|--------:|
#  |  0 | CABA        | tasa_empleo_18_65_mujeres |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_empleo_18_65_mujeres', new_value='Tasa de empleo femenino')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | CABA        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='prop_usa_lavarropas', new_value='Hogares con lavarropas')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | CABA        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='CABA', new_value='AR-C')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Buenos Aires', new_value='AR-B')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Catamarca', new_value='AR-K')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Córdoba', new_value='AR-X')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Corrientes', new_value='AR-W')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Chaco', new_value='AR-H')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Chubut', new_value='AR-U')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Entre Ríos', new_value='AR-E')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Formosa', new_value='AR-P')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Jujuy', new_value='AR-Y')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='La Pampa', new_value='AR-L')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='La Rioja', new_value='AR-F')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Mendoza', new_value='AR-M')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Misiones', new_value='AR-N')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Neuquén', new_value='AR-Q')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Río Negro', new_value='AR-R')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Salta', new_value='AR-A')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='San Juan', new_value='AR-J')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='San Luis', new_value='AR-D')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santa Cruz', new_value='AR-Z')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santa Fe', new_value='AR-S')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santiago del Estero', new_value='AR-G')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Tucumán', new_value='AR-T')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Tierra del Fuego', new_value='AR-V')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   prov_desc  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | prov_desc   | indicador               |   valor |
#  |---:|:------------|:------------------------|--------:|
#  |  0 | AR-C        | Tasa de empleo femenino |  75.518 |
#  
#  ------------------------------
#  