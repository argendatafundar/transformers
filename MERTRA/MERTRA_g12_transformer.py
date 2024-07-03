from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='provincia_id', axis=1),
	replace_value(col='indicador', curr_value='tasa_empleo', new_value='Tasa de empleo'),
	replace_value(col='indicador', curr_value='tasa_menor_18', new_value='Población menor de 18 años'),
	replace_value(col='provincia_desc', curr_value='CABA', new_value='AR-C'),
	replace_value(col='provincia_desc', curr_value='Buenos Aires', new_value='AR-B'),
	replace_value(col='provincia_desc', curr_value='Catamarca', new_value='AR-K'),
	replace_value(col='provincia_desc', curr_value='Córdoba', new_value='AR-X'),
	replace_value(col='provincia_desc', curr_value='Corrientes', new_value='AR-W'),
	replace_value(col='provincia_desc', curr_value='Chaco', new_value='AR-H'),
	replace_value(col='provincia_desc', curr_value='Chubut', new_value='AR-U'),
	replace_value(col='provincia_desc', curr_value='Entre Ríos', new_value='AR-E'),
	replace_value(col='provincia_desc', curr_value='Formosa', new_value='AR-P'),
	replace_value(col='provincia_desc', curr_value='Jujuy', new_value='AR-Y'),
	replace_value(col='provincia_desc', curr_value='La Pampa', new_value='AR-L'),
	replace_value(col='provincia_desc', curr_value='La Rioja', new_value='AR-F'),
	replace_value(col='provincia_desc', curr_value='Mendoza', new_value='AR-M'),
	replace_value(col='provincia_desc', curr_value='Misiones', new_value='AR-N'),
	replace_value(col='provincia_desc', curr_value='Neuquén', new_value='AR-Q'),
	replace_value(col='provincia_desc', curr_value='Río Negro', new_value='AR-R'),
	replace_value(col='provincia_desc', curr_value='Salta', new_value='AR-A'),
	replace_value(col='provincia_desc', curr_value='San Juan', new_value='AR-J'),
	replace_value(col='provincia_desc', curr_value='San Luis', new_value='AR-D'),
	replace_value(col='provincia_desc', curr_value='Santa Cruz', new_value='AR-Z'),
	replace_value(col='provincia_desc', curr_value='Santa Fe', new_value='AR-S'),
	replace_value(col='provincia_desc', curr_value='Santiago del Estero', new_value='AR-G'),
	replace_value(col='provincia_desc', curr_value='Tucumán', new_value='AR-T'),
	replace_value(col='provincia_desc', curr_value='Tierra del Fuego', new_value='AR-V'),
	multiplicar_por_escalar(col='tasa_empleo_18_65', k=100),
	rename_cols(map={'provincia_desc': 'geocodigo'}),
	wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='tasa_empleo_18_65', new_value='Tasa de empleo'),
	replace_value(col='indicador', curr_value='establecimientos_cada_1000_hab', new_value='Establecimientos productivos')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_id                    24 non-null     int64  
#   1   provincia_desc                  24 non-null     object 
#   2   establecimientos_cada_1000_hab  24 non-null     float64
#   3   tasa_empleo_18_65               24 non-null     float64
#  
#  |    |   provincia_id | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|---------------:|:-----------------|---------------------------------:|--------------------:|
#  |  0 |              2 | CABA             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  drop_col(col='provincia_id', axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | CABA             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_empleo', new_value='Tasa de empleo')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | CABA             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_menor_18', new_value='Población menor de 18 años')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | CABA             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='CABA', new_value='AR-C')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Buenos Aires', new_value='AR-B')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Catamarca', new_value='AR-K')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Córdoba', new_value='AR-X')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Corrientes', new_value='AR-W')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Chaco', new_value='AR-H')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Chubut', new_value='AR-U')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Entre Ríos', new_value='AR-E')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Formosa', new_value='AR-P')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Jujuy', new_value='AR-Y')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='La Pampa', new_value='AR-L')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='La Rioja', new_value='AR-F')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Mendoza', new_value='AR-M')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Misiones', new_value='AR-N')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Neuquén', new_value='AR-Q')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Río Negro', new_value='AR-R')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Salta', new_value='AR-A')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='San Juan', new_value='AR-J')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='San Luis', new_value='AR-D')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Santa Cruz', new_value='AR-Z')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Santa Fe', new_value='AR-S')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Santiago del Estero', new_value='AR-G')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Tucumán', new_value='AR-T')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |            0.789682 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia_desc', curr_value='Tierra del Fuego', new_value='AR-V')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |             78.9682 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo_18_65', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   provincia_desc                  24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | provincia_desc   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:-----------------|---------------------------------:|--------------------:|
#  |  0 | AR-C             |                          50.0578 |             78.9682 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_desc': 'geocodigo'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   geocodigo                       24 non-null     object 
#   1   establecimientos_cada_1000_hab  24 non-null     float64
#   2   tasa_empleo_18_65               24 non-null     float64
#  
#  |    | geocodigo   |   establecimientos_cada_1000_hab |   tasa_empleo_18_65 |
#  |---:|:------------|---------------------------------:|--------------------:|
#  |  0 | AR-C        |                          50.0578 |             78.9682 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                      |   valor |
#  |---:|:------------|:-------------------------------|--------:|
#  |  0 | AR-C        | establecimientos_cada_1000_hab | 50.0578 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='tasa_empleo_18_65', new_value='Tasa de empleo')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                      |   valor |
#  |---:|:------------|:-------------------------------|--------:|
#  |  0 | AR-C        | establecimientos_cada_1000_hab | 50.0578 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='establecimientos_cada_1000_hab', new_value='Establecimientos productivos')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                    |   valor |
#  |---:|:------------|:-----------------------------|--------:|
#  |  0 | AR-C        | Establecimientos productivos | 50.0578 |
#  
#  ------------------------------
#  