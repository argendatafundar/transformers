from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

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

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	multiplicar_por_escalar(col='tasa_empleo_mujer', k=100),
	wide_to_long(primary_keys=['provincia'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='tasa_empleo_mujer', new_value='Tasa de empleo femenino'),
	replace_value(col='indicador', curr_value='salario_relativo', new_value='Ingresos laborales'),
	replace_value(col='provincia', curr_value='Ciudad de Buenos Aires', new_value='AR-C'),
	replace_value(col='provincia', curr_value='Buenos Aires', new_value='AR-B'),
	replace_value(col='provincia', curr_value='Catamarca', new_value='AR-K'),
	replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X'),
	replace_value(col='provincia', curr_value='Corrientes', new_value='AR-W'),
	replace_value(col='provincia', curr_value='Chaco', new_value='AR-H'),
	replace_value(col='provincia', curr_value='Chubut', new_value='AR-U'),
	replace_value(col='provincia', curr_value='Entre Ríos', new_value='AR-E'),
	replace_value(col='provincia', curr_value='Formosa', new_value='AR-P'),
	replace_value(col='provincia', curr_value='Jujuy', new_value='AR-Y'),
	replace_value(col='provincia', curr_value='La Pampa', new_value='AR-L'),
	replace_value(col='provincia', curr_value='La Rioja', new_value='AR-F'),
	replace_value(col='provincia', curr_value='Mendoza', new_value='AR-M'),
	replace_value(col='provincia', curr_value='Misiones', new_value='AR-N'),
	replace_value(col='provincia', curr_value='Neuquén', new_value='AR-Q'),
	replace_value(col='provincia', curr_value='Río Negro', new_value='AR-R'),
	replace_value(col='provincia', curr_value='Salta', new_value='AR-A'),
	replace_value(col='provincia', curr_value='San Juan', new_value='AR-J'),
	replace_value(col='provincia', curr_value='San Luis', new_value='AR-D'),
	replace_value(col='provincia', curr_value='Santa Cruz', new_value='AR-Z'),
	replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S'),
	replace_value(col='provincia', curr_value='Santiago del Estero', new_value='AR-G'),
	replace_value(col='provincia', curr_value='Tucumán', new_value='AR-T'),
	replace_value(col='provincia', curr_value='Tierra del Fuego', new_value='AR-V'),
	rename_cols(map={'provincia': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 189 entries, 0 to 188
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               189 non-null    int64  
#   1   provincia          189 non-null    object 
#   2   salario_relativo   189 non-null    float64
#   3   tasa_empleo_mujer  189 non-null    float64
#  
#  |    |   anio | provincia    |   salario_relativo |   tasa_empleo_mujer |
#  |---:|-------:|:-------------|-------------------:|--------------------:|
#  |  0 |   2016 | Buenos Aires |            98.1688 |            0.542051 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 24 entries, 165 to 188
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   provincia          24 non-null     object 
#   1   salario_relativo   24 non-null     float64
#   2   tasa_empleo_mujer  24 non-null     float64
#  
#  |     | provincia    |   salario_relativo |   tasa_empleo_mujer |
#  |----:|:-------------|-------------------:|--------------------:|
#  | 165 | Buenos Aires |            100.367 |             62.5285 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo_mujer', k=100)
#  Index: 24 entries, 165 to 188
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   provincia          24 non-null     object 
#   1   salario_relativo   24 non-null     float64
#   2   tasa_empleo_mujer  24 non-null     float64
#  
#  |     | provincia    |   salario_relativo |   tasa_empleo_mujer |
#  |----:|:-------------|-------------------:|--------------------:|
#  | 165 | Buenos Aires |            100.367 |             62.5285 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['provincia'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia    | indicador        |   valor |
#  |---:|:-------------|:-----------------|--------:|
#  |  0 | Buenos Aires | salario_relativo | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_empleo_mujer', new_value='Tasa de empleo femenino')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia    | indicador        |   valor |
#  |---:|:-------------|:-----------------|--------:|
#  |  0 | Buenos Aires | salario_relativo | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='salario_relativo', new_value='Ingresos laborales')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia    | indicador          |   valor |
#  |---:|:-------------|:-------------------|--------:|
#  |  0 | Buenos Aires | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Ciudad de Buenos Aires', new_value='AR-C')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia    | indicador          |   valor |
#  |---:|:-------------|:-------------------|--------:|
#  |  0 | Buenos Aires | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Buenos Aires', new_value='AR-B')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Catamarca', new_value='AR-K')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Corrientes', new_value='AR-W')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chaco', new_value='AR-H')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chubut', new_value='AR-U')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Entre Ríos', new_value='AR-E')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Formosa', new_value='AR-P')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Jujuy', new_value='AR-Y')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Pampa', new_value='AR-L')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Rioja', new_value='AR-F')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Mendoza', new_value='AR-M')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Misiones', new_value='AR-N')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Neuquén', new_value='AR-Q')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Río Negro', new_value='AR-R')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Salta', new_value='AR-A')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Juan', new_value='AR-J')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Luis', new_value='AR-D')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Cruz', new_value='AR-Z')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santiago del Estero', new_value='AR-G')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tucumán', new_value='AR-T')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tierra del Fuego', new_value='AR-V')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo'})
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador          |   valor |
#  |---:|:------------|:-------------------|--------:|
#  |  0 | AR-B        | Ingresos laborales | 100.367 |
#  
#  ------------------------------
#  