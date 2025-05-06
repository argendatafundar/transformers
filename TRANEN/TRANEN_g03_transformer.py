from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG" & anio == anio.max()'),
	rename_cols(map={'fuente_energia': 'nivel2', 'porcentaje': 'valor', 'tipo_energia': 'nivel1'}),
	drop_col(col='anio', axis=1),
	drop_col(col='iso3', axis=1),
	drop_col(col='valor_en_twh', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 38938 entries, 0 to 38937
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            38938 non-null  int64  
#   1   iso3            38938 non-null  object 
#   2   fuente_energia  38938 non-null  object 
#   3   valor_en_twh    38938 non-null  float64
#   4   porcentaje      38938 non-null  float64
#   5   tipo_energia    38938 non-null  object 
#  
#  |    |   anio | iso3   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|:---------------|
#  |  0 |   1971 | DZA    | Otras renovables |              0 |            0 | Limpias        |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG" & anio == anio.max()')
#  Index: 9 entries, 111 to 34590
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            9 non-null      int64  
#   1   iso3            9 non-null      object 
#   2   fuente_energia  9 non-null      object 
#   3   valor_en_twh    9 non-null      float64
#   4   porcentaje      9 non-null      float64
#   5   tipo_energia    9 non-null      object 
#  
#  |     |   anio | iso3   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   |
#  |----:|-------:|:-------|:-----------------|---------------:|-------------:|:---------------|
#  | 111 |   2023 | ARG    | Otras renovables |        7.26455 |     0.712392 | Limpias        |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'nivel2', 'porcentaje': 'valor', 'tipo_energia': 'nivel1'})
#  Index: 9 entries, 111 to 34590
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          9 non-null      int64  
#   1   iso3          9 non-null      object 
#   2   nivel2        9 non-null      object 
#   3   valor_en_twh  9 non-null      float64
#   4   valor         9 non-null      float64
#   5   nivel1        9 non-null      object 
#  
#  |     |   anio | iso3   | nivel2           |   valor_en_twh |    valor | nivel1   |
#  |----:|-------:|:-------|:-----------------|---------------:|---------:|:---------|
#  | 111 |   2023 | ARG    | Otras renovables |        7.26455 | 0.712392 | Limpias  |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 9 entries, 111 to 34590
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          9 non-null      object 
#   1   nivel2        9 non-null      object 
#   2   valor_en_twh  9 non-null      float64
#   3   valor         9 non-null      float64
#   4   nivel1        9 non-null      object 
#  
#  |     | iso3   | nivel2           |   valor_en_twh |    valor | nivel1   |
#  |----:|:-------|:-----------------|---------------:|---------:|:---------|
#  | 111 | ARG    | Otras renovables |        7.26455 | 0.712392 | Limpias  |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 9 entries, 111 to 34590
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   nivel2        9 non-null      object 
#   1   valor_en_twh  9 non-null      float64
#   2   valor         9 non-null      float64
#   3   nivel1        9 non-null      object 
#  
#  |     | nivel2           |   valor_en_twh |    valor | nivel1   |
#  |----:|:-----------------|---------------:|---------:|:---------|
#  | 111 | Otras renovables |        7.26455 | 0.712392 | Limpias  |
#  
#  ------------------------------
#  
#  drop_col(col='valor_en_twh', axis=1)
#  Index: 9 entries, 111 to 34590
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel2  9 non-null      object 
#   1   valor   9 non-null      float64
#   2   nivel1  9 non-null      object 
#  
#  |     | nivel2           |    valor | nivel1   |
#  |----:|:-----------------|---------:|:---------|
#  | 111 | Otras renovables | 0.712392 | Limpias  |
#  
#  ------------------------------
#  