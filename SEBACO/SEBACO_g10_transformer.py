from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'rama': 'categoria', 'total_perc': 'valor'}),
	drop_col(col=['empleo', 'sbc_perc'], axis=1),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value='Ss. publicidad', new_value='Publicidad'),
	replace_value(col='categoria', curr_value='Ss. Arquitectura', new_value='Arquitectura'),
	replace_value(col='categoria', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rama        162 non-null    object 
#   1   anio        162 non-null    int64  
#   2   empleo      162 non-null    float64
#   3   sbc_perc    162 non-null    float64
#   4   total_perc  162 non-null    float64
#  
#  |    | rama                       |   anio |   empleo |   sbc_perc |   total_perc |
#  |---:|:---------------------------|-------:|---------:|-----------:|-------------:|
#  |  0 | Investigación y desarrollo |   1996 |     2549 |  0.0181982 |  0.000725962 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama': 'categoria', 'total_perc': 'valor'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   empleo     162 non-null    float64
#   3   sbc_perc   162 non-null    float64
#   4   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |   empleo |   sbc_perc |       valor |
#  |---:|:---------------------------|-------:|---------:|-----------:|------------:|
#  |  0 | Investigación y desarrollo |   1996 |     2549 |  0.0181982 | 0.000725962 |
#  
#  ------------------------------
#  
#  drop_col(col=['empleo', 'sbc_perc'], axis=1)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |     valor |
#  |---:|:---------------------------|-------:|----------:|
#  |  0 | Investigación y desarrollo |   1996 | 0.0725962 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |     valor |
#  |---:|:---------------------------|-------:|----------:|
#  |  0 | Investigación y desarrollo |   1996 | 0.0725962 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. publicidad', new_value='Publicidad')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |     valor |
#  |---:|:---------------------------|-------:|----------:|
#  |  0 | Investigación y desarrollo |   1996 | 0.0725962 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. Arquitectura', new_value='Arquitectura')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |     valor |
#  |---:|:---------------------------|-------:|----------:|
#  |  0 | Investigación y desarrollo |   1996 | 0.0725962 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | categoria                  |   anio |     valor |
#  |---:|:---------------------------|-------:|----------:|
#  |  0 | Investigación y desarrollo |   1996 | 0.0725962 |
#  
#  ------------------------------
#  