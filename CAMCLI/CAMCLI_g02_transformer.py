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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'continente_fundar': 'indicador', 'valor_en_ton': 'valor'}),
	agrupar_y_sumar(col_indicador='indicador', col_anio='anio'),
	mutiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24157 entries, 0 to 24156
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               24157 non-null  object 
#   1   continente_fundar  23863 non-null  object 
#   2   anio               24157 non-null  int64  
#   3   valor_en_ton       24157 non-null  float64
#  
#  |    | iso3   | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:-------|:--------------------|-------:|---------------:|
#  |  0 | AFG    | Asia                |   1949 |          14656 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 24157 entries, 0 to 24156
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  23863 non-null  object 
#   1   anio               24157 non-null  int64  
#   2   valor_en_ton       24157 non-null  float64
#  
#  |    | continente_fundar   |   anio |   valor_en_ton |
#  |---:|:--------------------|-------:|---------------:|
#  |  0 | Asia                |   1949 |          14656 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'indicador', 'valor_en_ton': 'valor'})
#  RangeIndex: 24157 entries, 0 to 24156
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  23863 non-null  object 
#   1   anio       24157 non-null  int64  
#   2   valor      24157 non-null  float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Asia        |   1949 |   14656 |
#  
#  ------------------------------
#  
#  agrupar_y_sumar(col_indicador='indicador', col_anio='anio')
#  RangeIndex: 1469 entries, 0 to 1468
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1469 non-null   object 
#   1   anio       1469 non-null   int64  
#   2   valor      1469 non-null   float64
#  
#  |    | indicador                              |   anio |    valor |
#  |---:|:---------------------------------------|-------:|---------:|
#  |  0 | América del Norte, Central y el Caribe |   1785 | 0.003664 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 1469 entries, 0 to 1468
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  1469 non-null   object 
#   1   anio       1469 non-null   int64  
#   2   valor      1469 non-null   float64
#  
#  |    | indicador                              |   anio |    valor |
#  |---:|:---------------------------------------|-------:|---------:|
#  |  0 | América del Norte, Central y el Caribe |   1785 | 0.003664 |
#  
#  ------------------------------
#  