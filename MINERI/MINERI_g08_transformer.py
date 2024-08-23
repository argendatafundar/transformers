from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def media_doce_meses_indicador(df: DataFrame, indicador_col = 'indicador', anio_col = 'anio', value_col = 'valor'):
    from pandas import to_datetime

    result = {'anio': [], 'indicador': [], 'valor': []}
    df[anio_col] = to_datetime(df[anio_col])
    df[value_col] = df[value_col].astype(float)
    for indicador, group in df.groupby(indicador_col):
        group = group.drop(columns = indicador_col)

        for yyyy, anio in  group.groupby(group.anio.dt.year):
            result['indicador'].append(indicador)
            result['anio'].append(yyyy)
            result['valor'].append(anio.valor.mean())

    result = DataFrame(result)
    return result

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'variable': 'indicador', 'fecha': 'anio'}),
	media_doce_meses_indicador(indicador_col='indicador', anio_col='anio', value_col='valor'),
	query(condition="indicador not in ('oro', 'litio', 'plata', 'cobre')"),
	replace_values(col='indicador', values={'indice_cobre': 'Cobre', 'indice_litio': 'Litio', 'indice_oro': 'Oro', 'indice_plata': 'Plata'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4736 entries, 0 to 4735
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   fecha     4736 non-null   object 
#   1   variable  4736 non-null   object 
#   2   valor     4736 non-null   float64
#  
#  |    | fecha      | variable   |   valor |
#  |---:|:-----------|:-----------|--------:|
#  |  0 | 1960-01-01 | oro        |   35.27 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'indicador', 'fecha': 'anio'})
#  RangeIndex: 4736 entries, 0 to 4735
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype         
#  ---  ------     --------------  -----         
#   0   anio       4736 non-null   datetime64[ns]
#   1   indicador  4736 non-null   object        
#   2   valor      4736 non-null   float64       
#  
#  |    | anio                | indicador   |   valor |
#  |---:|:--------------------|:------------|--------:|
#  |  0 | 1960-01-01 00:00:00 | oro         |   35.27 |
#  
#  ------------------------------
#  
#  media_doce_meses_indicador(indicador_col='indicador', anio_col='anio', value_col='valor')
#  RangeIndex: 402 entries, 0 to 401
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       402 non-null    int64  
#   1   indicador  402 non-null    object 
#   2   valor      402 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1960 | cobre       | 678.756 |
#  
#  ------------------------------
#  
#  query(condition="indicador not in ('oro', 'litio', 'plata', 'cobre')")
#  Index: 201 entries, 65 to 265
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       201 non-null    int64  
#   1   indicador  201 non-null    object 
#   2   valor      201 non-null    float64
#  
#  |    |   anio | indicador    |   valor |
#  |---:|-------:|:-------------|--------:|
#  | 65 |   1960 | indice_cobre | 10.9549 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'indice_cobre': 'Cobre', 'indice_litio': 'Litio', 'indice_oro': 'Oro', 'indice_plata': 'Plata'})
#  Index: 201 entries, 65 to 265
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       201 non-null    int64  
#   1   indicador  201 non-null    object 
#   2   valor      201 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  | 65 |   1960 | Cobre       | 10.9549 |
#  
#  ------------------------------
#  