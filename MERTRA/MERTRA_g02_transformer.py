from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3_desc', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'ratio_tasa_actividad_mujer_varon': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 5 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   iso3                              16704 non-null  object 
#   1   iso3_desc                         16704 non-null  object 
#   2   anio                              16704 non-null  int64  
#   3   ratio_tasa_actividad_mujer_varon  5027 non-null   float64
#   4   nivel_agregacion                  16704 non-null  object 
#  
#  |    | iso3   | iso3_desc                   |   anio |   ratio_tasa_actividad_mujer_varon | nivel_agregacion   |
#  |---:|:-------|:----------------------------|-------:|-----------------------------------:|:-------------------|
#  |  0 | AFE    | Africa Eastern and Southern |   2023 |                                nan | agregacion         |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc', axis=1)
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 4 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   iso3                              16704 non-null  object 
#   1   anio                              16704 non-null  int64  
#   2   ratio_tasa_actividad_mujer_varon  5027 non-null   float64
#   3   nivel_agregacion                  16704 non-null  object 
#  
#  |    | iso3   |   anio |   ratio_tasa_actividad_mujer_varon | nivel_agregacion   |
#  |---:|:-------|-------:|-----------------------------------:|:-------------------|
#  |  0 | AFE    |   2023 |                                nan | agregacion         |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 3 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   iso3                              16704 non-null  object 
#   1   anio                              16704 non-null  int64  
#   2   ratio_tasa_actividad_mujer_varon  5027 non-null   float64
#  
#  |    | iso3   |   anio |   ratio_tasa_actividad_mujer_varon |
#  |---:|:-------|-------:|-----------------------------------:|
#  |  0 | AFE    |   2023 |                                nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'ratio_tasa_actividad_mujer_varon': 'valor'})
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  16704 non-null  object 
#   1   anio       16704 non-null  int64  
#   2   valor      5027 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   2023 |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 5027 entries, 65 to 16681
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  5027 non-null   object 
#   1   anio       5027 non-null   int64  
#   2   valor      5027 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  | 65 | AFW         |   2022 |  87.371 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 5027 entries, 0 to 5026
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  5027 non-null   object 
#   1   anio       5027 non-null   int64  
#   2   valor      5027 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1960 | 32.2957 |
#  
#  ------------------------------
#  