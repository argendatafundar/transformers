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
#  RangeIndex: 67977 entries, 0 to 67976
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               59514 non-null  object 
#   1   continente_fundar  58968 non-null  object 
#   2   anio               67977 non-null  int64  
#   3   valor_en_ton       67977 non-null  float64
#  
#  |    | iso3   | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:-------|:--------------------|-------:|---------------:|
#  |  0 | AFG    | Asia                |   1750 |              0 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 67977 entries, 0 to 67976
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  58968 non-null  object 
#   1   anio               67977 non-null  int64  
#   2   valor_en_ton       67977 non-null  float64
#  
#  |    | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:--------------------|-------:|---------------:|
#  |  0 | Asia                |   1750 |              0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'indicador', 'valor_en_ton': 'valor'})
#  RangeIndex: 67977 entries, 0 to 67976
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  58968 non-null  object 
#   1   anio       67977 non-null  int64  
#   2   valor      67977 non-null  float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Asia        |   1750 |       0 |
#  
#  ------------------------------
#  
#  agrupar_y_sumar(col_indicador='indicador', col_anio='anio')
#  RangeIndex: 1911 entries, 0 to 1910
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1911 non-null   object 
#   1   anio       1911 non-null   int64  
#   2   valor      1911 non-null   float64
#  
#  |    | indicador                              |   anio |   valor |
#  |---:|:---------------------------------------|-------:|--------:|
#  |  0 | América del Norte, Central y el Caribe |   1750 |       0 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 1911 entries, 0 to 1910
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1911 non-null   object 
#   1   anio       1911 non-null   int64  
#   2   valor      1911 non-null   float64
#  
#  |    | indicador                              |   anio |   valor |
#  |---:|:---------------------------------------|-------:|--------:|
#  |  0 | América del Norte, Central y el Caribe |   1750 |       0 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Asia': 0, 'América del Norte, Central y el Caribe': 1, 'Europa': 2, 'África': 3, 'América del Sur': 4, 'Transporte Internacional': 6, 'Oceanía': 5}, prefix=['anio'], suffix=[])
#  Index: 1911 entries, 546 to 1637
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1911 non-null   object 
#   1   anio       1911 non-null   int64  
#   2   valor      1911 non-null   float64
#  
#  |     | indicador   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 546 | Asia        |   1750 |       0 |
#  
#  ------------------------------
#  