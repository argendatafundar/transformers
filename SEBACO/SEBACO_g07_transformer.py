from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'prop': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'provincia'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop': 'valor'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | provincia   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | CABA        |   1996 | 41.0583 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | provincia   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | CABA        |   1996 | 41.0583 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'provincia'])
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | provincia   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | CABA        |   1996 | 41.0583 |
#  
#  ------------------------------
#  