from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'geocodigoFundar': 'geocodigo', 'geonombreFundar': 'geonombre'}),
	multiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'geonombre'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  432 non-null    object 
#   1   geonombreFundar  432 non-null    object 
#   2   anio             432 non-null    int64  
#   3   valor            432 non-null    float64
#   4   cod_pcia         432 non-null    int64  
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |   cod_pcia |
#  |---:|:------------------|:------------------|-------:|--------:|-----------:|
#  |  0 | AR-C              | CABA              |   2004 |  0.0021 |          2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo', 'geonombreFundar': 'geonombre'})
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  432 non-null    object 
#   1   geonombre  432 non-null    object 
#   2   anio       432 non-null    int64  
#   3   valor      432 non-null    float64
#   4   cod_pcia   432 non-null    int64  
#  
#  |    | geocodigo   | geonombre   |   anio |   valor |   cod_pcia |
#  |---:|:------------|:------------|-------:|--------:|-----------:|
#  |  0 | AR-C        | CABA        |   2004 |    0.21 |          2 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  432 non-null    object 
#   1   geonombre  432 non-null    object 
#   2   anio       432 non-null    int64  
#   3   valor      432 non-null    float64
#   4   cod_pcia   432 non-null    int64  
#  
#  |    | geocodigo   | geonombre   |   anio |   valor |   cod_pcia |
#  |---:|:------------|:------------|-------:|--------:|-----------:|
#  |  0 | AR-C        | CABA        |   2004 |    0.21 |          2 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geonombre'])
#  RangeIndex: 432 entries, 0 to 431
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  432 non-null    object 
#   1   geonombre  432 non-null    object 
#   2   anio       432 non-null    int64  
#   3   valor      432 non-null    float64
#   4   cod_pcia   432 non-null    int64  
#  
#  |    | geocodigo   | geonombre    |   anio |   valor |   cod_pcia |
#  |---:|:------------|:-------------|-------:|--------:|-----------:|
#  |  0 | AR-B        | Buenos Aires |   2004 |    8.89 |          6 |
#  
#  ------------------------------
#  