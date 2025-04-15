from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def agrupar_y_sumar(df: DataFrame, col_indicador, col_anio):
    return df.groupby([col_indicador, col_anio]).sum().reset_index()

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'continente_fundar': 'indicador', 'valor_en_ton': 'valor'}),
	agrupar_y_sumar(col_indicador='indicador', col_anio='anio'),
	multiplicar_por_escalar(col='valor', k=1e-06),
	sort_values_by_comparison(colname='indicador', precedence={'Asia': 0, 'América del Norte, Central y el Caribe': 1, 'Europa': 2, 'África': 3, 'América del Sur': 4, 'Transporte Internacional': 6, 'Oceanía': 5}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23645 entries, 0 to 23644
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               23645 non-null  object 
#   1   continente_fundar  23186 non-null  object 
#   2   anio               23645 non-null  int64  
#   3   valor_en_ton       23645 non-null  float64
#  
#  |    | iso3   | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:-------|:--------------------|-------:|---------------:|
#  |  0 | AFG    | Asia                |   1949 |          14656 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 23645 entries, 0 to 23644
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  23186 non-null  object 
#   1   anio               23645 non-null  int64  
#   2   valor_en_ton       23645 non-null  float64
#  
#  |    | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:--------------------|-------:|---------------:|
#  |  0 | Asia                |   1949 |          14656 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'indicador', 'valor_en_ton': 'valor'})
#  RangeIndex: 23645 entries, 0 to 23644
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  23186 non-null  object 
#   1   anio       23645 non-null  int64  
#   2   valor      23645 non-null  float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Asia        |   1949 |   14656 |
#  
#  ------------------------------
#  
#  agrupar_y_sumar(col_indicador='indicador', col_anio='anio')
#  RangeIndex: 1381 entries, 0 to 1380
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1381 non-null   object 
#   1   anio       1381 non-null   int64  
#   2   valor      1381 non-null   float64
#  
#  |    | indicador                              |   anio |    valor |
#  |---:|:---------------------------------------|-------:|---------:|
#  |  0 | América del Norte, Central y el Caribe |   1785 | 0.003664 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 1381 entries, 0 to 1380
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1381 non-null   object 
#   1   anio       1381 non-null   int64  
#   2   valor      1381 non-null   float64
#  
#  |    | indicador                              |   anio |    valor |
#  |---:|:---------------------------------------|-------:|---------:|
#  |  0 | América del Norte, Central y el Caribe |   1785 | 0.003664 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Asia': 0, 'América del Norte, Central y el Caribe': 1, 'Europa': 2, 'África': 3, 'América del Sur': 4, 'Transporte Internacional': 6, 'Oceanía': 5}, prefix=['anio'], suffix=[])
#  Index: 1381 entries, 419 to 1240
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1381 non-null   object 
#   1   anio       1381 non-null   int64  
#   2   valor      1381 non-null   float64
#  
#  |     | indicador   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 419 | Asia        |   1750 |       0 |
#  
#  ------------------------------
#  