from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector_desc': 'indicador', 'share_empleo': 'valor'}),
	drop_col(col=['gran_sector', 'sector_codigo', 'empleo_miles'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100),
	drop_na(col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 7 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           42228 non-null  object 
#   1   anio           42228 non-null  int64  
#   2   gran_sector    42228 non-null  object 
#   3   sector_codigo  42228 non-null  object 
#   4   sector_desc    42228 non-null  object 
#   5   empleo_miles   30958 non-null  float64
#   6   share_empleo   30953 non-null  float64
#  
#  |    | iso3   |   anio | gran_sector   | sector_codigo   | sector_desc   |   empleo_miles |   share_empleo |
#  |---:|:-------|-------:|:--------------|:----------------|:--------------|---------------:|---------------:|
#  |  0 | ARG    |   1950 | Bienes        | A               | Agro y pesca  |        1676.85 |       0.258262 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector_desc': 'indicador', 'share_empleo': 'valor'})
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 7 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           42228 non-null  object 
#   1   anio           42228 non-null  int64  
#   2   gran_sector    42228 non-null  object 
#   3   sector_codigo  42228 non-null  object 
#   4   indicador      42228 non-null  object 
#   5   empleo_miles   30958 non-null  float64
#   6   valor          30953 non-null  float64
#  
#  |    | iso3   |   anio | gran_sector   | sector_codigo   | indicador    |   empleo_miles |    valor |
#  |---:|:-------|-------:|:--------------|:----------------|:-------------|---------------:|---------:|
#  |  0 | ARG    |   1950 | Bienes        | A               | Agro y pesca |        1676.85 | 0.258262 |
#  
#  ------------------------------
#  
#  drop_col(col=['gran_sector', 'sector_codigo', 'empleo_miles'], axis=1)
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       42228 non-null  object 
#   1   anio       42228 non-null  int64  
#   2   indicador  42228 non-null  object 
#   3   valor      30953 non-null  float64
#  
#  |    | iso3   |   anio | indicador    |   valor |
#  |---:|:-------|-------:|:-------------|--------:|
#  |  0 | ARG    |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 42228 entries, 0 to 42227
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       42228 non-null  object 
#   1   anio       42228 non-null  int64  
#   2   indicador  42228 non-null  object 
#   3   valor      30953 non-null  float64
#  
#  |    | iso3   |   anio | indicador    |   valor |
#  |---:|:-------|-------:|:-------------|--------:|
#  |  0 | ARG    |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 30953 entries, 0 to 42227
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       30953 non-null  object 
#   1   anio       30953 non-null  int64  
#   2   indicador  30953 non-null  object 
#   3   valor      30953 non-null  float64
#  
#  |    | iso3   |   anio | indicador    |   valor |
#  |---:|:-------|-------:|:-------------|--------:|
#  |  0 | ARG    |   1950 | Agro y pesca | 25.8262 |
#  
#  ------------------------------
#  