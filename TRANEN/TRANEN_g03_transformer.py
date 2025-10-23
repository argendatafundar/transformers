from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG" & anio == anio.max()'),
	rename_cols(map={'fuente_energia': 'nivel2', 'porcentaje': 'valor', 'tipo_energia': 'nivel1'}),
	drop_col(col='anio', axis=1),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='valor_en_twh', axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 40851 entries, 0 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             40851 non-null  int64  
#   1   geocodigoFundar  40851 non-null  object 
#   2   fuente_energia   40851 non-null  object 
#   3   valor_en_twh     40851 non-null  float64
#   4   porcentaje       40851 non-null  float64
#   5   tipo_energia     40851 non-null  object 
#   6   geonombreFundar  40851 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  |  0 |   1965 | DZA               | Otras renovables |              0 |            0 | Limpias        | Argelia           |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG" & anio == anio.max()')
#  Index: 9 entries, 119 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             9 non-null      int64  
#   1   geocodigoFundar  9 non-null      object 
#   2   fuente_energia   9 non-null      object 
#   3   valor_en_twh     9 non-null      float64
#   4   porcentaje       9 non-null      float64
#   5   tipo_energia     9 non-null      object 
#   6   geonombreFundar  9 non-null      object 
#  
#  |     |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 119 |   2024 | ARG               | Otras renovables |        8.23241 |     0.844324 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'nivel2', 'porcentaje': 'valor', 'tipo_energia': 'nivel1'})
#  Index: 9 entries, 119 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             9 non-null      int64  
#   1   geocodigoFundar  9 non-null      object 
#   2   nivel2           9 non-null      object 
#   3   valor_en_twh     9 non-null      float64
#   4   valor            9 non-null      float64
#   5   nivel1           9 non-null      object 
#   6   geonombreFundar  9 non-null      object 
#  
#  |     |   anio | geocodigoFundar   | nivel2           |   valor_en_twh |    valor | nivel1   | geonombreFundar   |
#  |----:|-------:|:------------------|:-----------------|---------------:|---------:|:---------|:------------------|
#  | 119 |   2024 | ARG               | Otras renovables |        8.23241 | 0.844324 | Limpias  | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 9 entries, 119 to 36431
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9 non-null      object 
#   1   nivel2           9 non-null      object 
#   2   valor_en_twh     9 non-null      float64
#   3   valor            9 non-null      float64
#   4   nivel1           9 non-null      object 
#   5   geonombreFundar  9 non-null      object 
#  
#  |     | geocodigoFundar   | nivel2           |   valor_en_twh |    valor | nivel1   | geonombreFundar   |
#  |----:|:------------------|:-----------------|---------------:|---------:|:---------|:------------------|
#  | 119 | ARG               | Otras renovables |        8.23241 | 0.844324 | Limpias  | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 9 entries, 119 to 36431
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   nivel2           9 non-null      object 
#   1   valor_en_twh     9 non-null      float64
#   2   valor            9 non-null      float64
#   3   nivel1           9 non-null      object 
#   4   geonombreFundar  9 non-null      object 
#  
#  |     | nivel2           |   valor_en_twh |    valor | nivel1   | geonombreFundar   |
#  |----:|:-----------------|---------------:|---------:|:---------|:------------------|
#  | 119 | Otras renovables |        8.23241 | 0.844324 | Limpias  | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='valor_en_twh', axis=1)
#  Index: 9 entries, 119 to 36431
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   nivel2           9 non-null      object 
#   1   valor            9 non-null      float64
#   2   nivel1           9 non-null      object 
#   3   geonombreFundar  9 non-null      object 
#  
#  |     | nivel2           |    valor | nivel1   | geonombreFundar   |
#  |----:|:-----------------|---------:|:---------|:------------------|
#  | 119 | Otras renovables | 0.844324 | Limpias  | Argentina         |
#  
#  ------------------------------
#  