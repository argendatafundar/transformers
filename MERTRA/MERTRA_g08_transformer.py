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

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'tasa_empleo': 'valor'}),
	replace_value(col='provincia', curr_value='CABA', new_value='AR-C'),
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
	replace_value(col='provincia', curr_value='Total país', new_value='MERTRA_TOTAL'),
	rename_cols(map={'provincia': 'geocodigo'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 197 entries, 0 to 196
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         197 non-null    int64  
#   1   provincia    197 non-null    object 
#   2   tasa_empleo  197 non-null    float64
#  
#  |    |   anio | provincia   |   tasa_empleo |
#  |---:|-------:|:------------|--------------:|
#  |  0 |   2016 | CABA        |      0.513004 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   provincia    25 non-null     object 
#   1   tasa_empleo  25 non-null     float64
#  
#  |     | provincia   |   tasa_empleo |
#  |----:|:------------|--------------:|
#  | 165 | CABA        |      0.528291 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tasa_empleo': 'valor'})
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | CABA        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='CABA', new_value='AR-C')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Buenos Aires', new_value='AR-B')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Catamarca', new_value='AR-K')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Corrientes', new_value='AR-W')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chaco', new_value='AR-H')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Chubut', new_value='AR-U')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Entre Ríos', new_value='AR-E')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Formosa', new_value='AR-P')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Jujuy', new_value='AR-Y')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Pampa', new_value='AR-L')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='La Rioja', new_value='AR-F')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Mendoza', new_value='AR-M')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Misiones', new_value='AR-N')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Neuquén', new_value='AR-Q')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Río Negro', new_value='AR-R')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Salta', new_value='AR-A')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Juan', new_value='AR-J')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='San Luis', new_value='AR-D')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Cruz', new_value='AR-Z')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santiago del Estero', new_value='AR-G')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tucumán', new_value='AR-T')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Tierra del Fuego', new_value='AR-V')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Total país', new_value='MERTRA_TOTAL')
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | provincia   |    valor |
#  |----:|:------------|---------:|
#  | 165 | AR-C        | 0.528291 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo'})
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | geocodigo   |   valor |
#  |----:|:------------|--------:|
#  | 165 | AR-C        | 52.8291 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 25 entries, 165 to 196
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  25 non-null     object 
#   1   valor      25 non-null     float64
#  
#  |     | geocodigo   |   valor |
#  |----:|:------------|--------:|
#  | 165 | AR-C        | 52.8291 |
#  
#  ------------------------------
#  