from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
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
wide_to_long(primary_keys=['anio', 'provincia'], value_name='valor', var_name='indicador'),
	latest_year(by='anio'),
	replace_value(col='indicador', curr_value='tasa_empleo', new_value='Tasa de empleo'),
	replace_value(col='indicador', curr_value='tasa_menor_18', new_value='Población menor de 18 años'),
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
	replace_value(col='provincia', curr_value='Tierra del Fuego', new_value='AR-V')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           165 non-null    int64  
#   1   provincia      165 non-null    object 
#   2   tasa_empleo    165 non-null    float64
#   3   tasa_menor_18  165 non-null    float64
#  
#  |    |   anio | provincia              |   tasa_empleo |   tasa_menor_18 |
#  |---:|-------:|:-----------------------|--------------:|----------------:|
#  |  0 |   2016 | Ciudad de Buenos Aires |      0.513004 |        0.188306 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'provincia'], value_name='valor', var_name='indicador')
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       330 non-null    int64  
#   1   provincia  330 non-null    object 
#   2   indicador  330 non-null    object 
#   3   valor      330 non-null    float64
#  
#  |    |   anio | provincia              | indicador   |    valor |
#  |---:|-------:|:-----------------------|:------------|---------:|
#  |  0 |   2016 | Ciudad de Buenos Aires | tasa_empleo | 0.513004 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia              | indicador   |    valor |
#  |---:|:-----------------------|:------------|---------:|
#  |  6 | Ciudad de Buenos Aires | tasa_empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_empleo', new_value='Tasa de empleo')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia              | indicador      |    valor |
#  |---:|:-----------------------|:---------------|---------:|
#  |  6 | Ciudad de Buenos Aires | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_menor_18', new_value='Población menor de 18 años')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia              | indicador      |    valor |
#  |---:|:-----------------------|:---------------|---------:|
#  |  6 | Ciudad de Buenos Aires | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Ciudad de Buenos Aires', new_value='AR-C')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Buenos Aires', new_value='AR-B')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Catamarca', new_value='AR-K')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Corrientes', new_value='AR-W')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chaco', new_value='AR-H')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chubut', new_value='AR-U')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Entre Ríos', new_value='AR-E')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Formosa', new_value='AR-P')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Jujuy', new_value='AR-Y')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Pampa', new_value='AR-L')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Rioja', new_value='AR-F')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Mendoza', new_value='AR-M')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Misiones', new_value='AR-N')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Neuquén', new_value='AR-Q')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Río Negro', new_value='AR-R')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Salta', new_value='AR-A')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Juan', new_value='AR-J')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Luis', new_value='AR-D')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Cruz', new_value='AR-Z')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santiago del Estero', new_value='AR-G')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tucumán', new_value='AR-T')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tierra del Fuego', new_value='AR-V')
#  Index: 48 entries, 6 to 329
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | provincia   | indicador      |    valor |
#  |---:|:------------|:---------------|---------:|
#  |  6 | AR-C        | Tasa de empleo | 0.510064 |
#  
#  ------------------------------
#  