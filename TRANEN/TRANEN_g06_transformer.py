from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar == 'ARG'"),
	query(condition="tipo_energia == 'Limpias'"),
	rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
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
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG               | Otras renovables |              0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  query(condition="tipo_energia == 'Limpias'")
#  Index: 360 entries, 60 to 22814
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   fuente_energia   360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   tipo_energia     360 non-null    object 
#   6   geonombreFundar  360 non-null    object 
#  
#  |    |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG               | Otras renovables |              0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'})
#  Index: 360 entries, 60 to 22814
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigo        360 non-null    object 
#   2   indicador        360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   valor            360 non-null    float64
#   5   tipo_energia     360 non-null    object 
#   6   geonombreFundar  360 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor_en_twh |   valor | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|---------------:|--------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |              0 |       0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 360 entries, 60 to 22814
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigo        360 non-null    object 
#   2   indicador        360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   valor            360 non-null    float64
#   5   tipo_energia     360 non-null    object 
#   6   geonombreFundar  360 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor_en_twh |   valor | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|---------------:|--------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |              0 |       0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigo        360 non-null    object 
#   2   indicador        360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   valor            360 non-null    float64
#   5   tipo_energia     360 non-null    object 
#   6   geonombreFundar  360 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor_en_twh |   valor | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|---------------:|--------:|:---------------|:------------------|
#  |  0 |   1965 | ARG         | Otras renovables |              0 |       0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  