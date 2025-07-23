from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'descripcion': 'indicador', 'prop_sobre_sbc_expo': 'valor'}),
	sort_values(how='ascending', by=['anio', 'indicador']),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 102 non-null    int64  
#   1   descripcion          102 non-null    object 
#   2   prop_sobre_sbc_expo  102 non-null    float64
#  
#  |    |   anio | descripcion           |   prop_sobre_sbc_expo |
#  |---:|-------:|:----------------------|----------------------:|
#  |  0 |   2006 | Propiedad intelectual |             0.0319314 |
#  
#  ------------------------------
#  
#  rename_cols(map={'descripcion': 'indicador', 'prop_sobre_sbc_expo': 'valor'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador             |     valor |
#  |---:|-------:|:----------------------|----------:|
#  |  0 |   2006 | Propiedad intelectual | 0.0319314 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   2006 | Investigación y desarrollo | 4.89056 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador                  |   valor |
#  |---:|-------:|:---------------------------|--------:|
#  |  0 |   2006 | Investigación y desarrollo | 4.89056 |
#  
#  ------------------------------
#  