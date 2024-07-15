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
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
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
	replace_value(col='prov_desc', curr_value='Tierra del Fuego', new_value='AR-V'),
	replace_value(col='prov_desc', curr_value='CABA', new_value='AR-C'),
	rename_cols(map={'apertura_edad': 'indicador', 'tasa_empleo': 'valor', 'prov_desc': 'geocodigo'}),
	query(condition="indicador in ('Edad, entre 18 y 65', 'Edad, total')"),
	replace_value(col='indicador', curr_value='Edad, entre 18 y 65', new_value='Población entre 18 y 65 años'),
	replace_value(col='indicador', curr_value='Edad, total', new_value='Población total'),
	multiplicar_por_escalar(col='valor', k=100),
	query(condition='geocodigo != "Total"')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 788 entries, 0 to 787
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           788 non-null    int64  
#   1   prov_desc      788 non-null    object 
#   2   apertura_edad  788 non-null    object 
#   3   tasa_empleo    788 non-null    float64
#  
#  |    |   anio | prov_desc    | apertura_edad       |   tasa_empleo |
#  |---:|-------:|:-------------|:--------------------|--------------:|
#  |  0 |   2016 | Buenos Aires | Edad, entre 18 y 65 |      0.656322 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc    | apertura_edad       |   tasa_empleo |
#  |----:|:-------------|:--------------------|--------------:|
#  | 688 | Buenos Aires | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Buenos Aires', new_value='AR-B')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Catamarca', new_value='AR-K')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Córdoba', new_value='AR-X')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Corrientes', new_value='AR-W')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Chaco', new_value='AR-H')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Chubut', new_value='AR-U')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Entre Ríos', new_value='AR-E')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Formosa', new_value='AR-P')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Jujuy', new_value='AR-Y')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='La Pampa', new_value='AR-L')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='La Rioja', new_value='AR-F')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Mendoza', new_value='AR-M')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Misiones', new_value='AR-N')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Neuquén', new_value='AR-Q')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Río Negro', new_value='AR-R')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Salta', new_value='AR-A')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='San Juan', new_value='AR-J')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='San Luis', new_value='AR-D')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santa Cruz', new_value='AR-Z')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santa Fe', new_value='AR-S')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Santiago del Estero', new_value='AR-G')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Tucumán', new_value='AR-T')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='Tierra del Fuego', new_value='AR-V')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='prov_desc', curr_value='CABA', new_value='AR-C')
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   prov_desc      100 non-null    object 
#   1   apertura_edad  100 non-null    object 
#   2   tasa_empleo    100 non-null    float64
#  
#  |     | prov_desc   | apertura_edad       |   tasa_empleo |
#  |----:|:------------|:--------------------|--------------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 |      0.714799 |
#  
#  ------------------------------
#  
#  rename_cols(map={'apertura_edad': 'indicador', 'tasa_empleo': 'valor', 'prov_desc': 'geocodigo'})
#  Index: 100 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  100 non-null    object 
#   1   indicador  100 non-null    object 
#   2   valor      100 non-null    float64
#  
#  |     | geocodigo   | indicador           |    valor |
#  |----:|:------------|:--------------------|---------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 | 0.714799 |
#  
#  ------------------------------
#  
#  query(condition="indicador in ('Edad, entre 18 y 65', 'Edad, total')")
#  Index: 50 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  50 non-null     object 
#   1   indicador  50 non-null     object 
#   2   valor      50 non-null     float64
#  
#  |     | geocodigo   | indicador           |    valor |
#  |----:|:------------|:--------------------|---------:|
#  | 688 | AR-B        | Edad, entre 18 y 65 | 0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Edad, entre 18 y 65', new_value='Población entre 18 y 65 años')
#  Index: 50 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  50 non-null     object 
#   1   indicador  50 non-null     object 
#   2   valor      50 non-null     float64
#  
#  |     | geocodigo   | indicador                    |    valor |
#  |----:|:------------|:-----------------------------|---------:|
#  | 688 | AR-B        | Población entre 18 y 65 años | 0.714799 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Edad, total', new_value='Población total')
#  Index: 50 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  50 non-null     object 
#   1   indicador  50 non-null     object 
#   2   valor      50 non-null     float64
#  
#  |     | geocodigo   | indicador                    |   valor |
#  |----:|:------------|:-----------------------------|--------:|
#  | 688 | AR-B        | Población entre 18 y 65 años | 71.4799 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 50 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  50 non-null     object 
#   1   indicador  50 non-null     object 
#   2   valor      50 non-null     float64
#  
#  |     | geocodigo   | indicador                    |   valor |
#  |----:|:------------|:-----------------------------|--------:|
#  | 688 | AR-B        | Población entre 18 y 65 años | 71.4799 |
#  
#  ------------------------------
#  
#  query(condition='geocodigo != "Total"')
#  Index: 48 entries, 688 to 787
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |     | geocodigo   | indicador                    |   valor |
#  |----:|:------------|:-----------------------------|--------:|
#  | 688 | AR-B        | Población entre 18 y 65 años | 71.4799 |
#  
#  ------------------------------
#  