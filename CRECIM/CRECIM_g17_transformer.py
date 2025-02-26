from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	query(condition="geocodigo not in ('LAC', 'TLA', 'DESHUM_ZZH.LAC', 'DESHUM_AHDI.LAC')"),
	sort_values(how='ascending', by='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3255 entries, 0 to 3254
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             3255 non-null   object 
#   1   anio             3255 non-null   int64  
#   2   cambio_relativo  3255 non-null   float64
#  
#  |    | iso3   |   anio |   cambio_relativo |
#  |---:|:-------|-------:|------------------:|
#  |  0 | AFE    |   2023 |         -0.021853 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 3255 entries, 0 to 3254
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3255 non-null   object 
#   1   anio       3255 non-null   int64  
#   2   valor      3255 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   2023 | -2.1853 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 3255 entries, 0 to 3254
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3255 non-null   object 
#   1   anio       3255 non-null   int64  
#   2   valor      3255 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   2023 | -2.1853 |
#  
#  ------------------------------
#  
#  query(condition="geocodigo not in ('LAC', 'TLA', 'DESHUM_ZZH.LAC', 'DESHUM_AHDI.LAC')")
#  Index: 3229 entries, 0 to 3254
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3229 non-null   object 
#   1   anio       3229 non-null   int64  
#   2   valor      3229 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFE         |   2023 | -2.1853 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by='anio')
#  RangeIndex: 3229 entries, 0 to 3228
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3229 non-null   object 
#   1   anio       3229 non-null   int64  
#   2   valor      3229 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ZWE         |   2011 |       0 |
#  
#  ------------------------------
#  