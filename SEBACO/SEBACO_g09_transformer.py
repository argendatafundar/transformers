from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
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
rename_cols(map={'rama': 'indicador'}),
	rename_cols(map={'prop_rama': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='Ss. publicidad', new_value='Publicidad'),
	replace_value(col='indicador', curr_value='Ss. Arquitectura', new_value='Arquitectura'),
	replace_value(col='indicador', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables'),
	sort_values_by_comparison(colname='indicador', precedence={'Investigación y desarrollo': 0, 'Publicidad': 1, 'Otras': 5, 'Arquitectura': 2, 'Jurídicos y contables': 3, 'SSI': 4}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   rama       162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop_rama  162 non-null    float64
#  
#  |    | rama                       |   anio |   prop_rama |
#  |---:|:---------------------------|-------:|------------:|
#  |  0 | Investigación y desarrollo |   1996 |   0.0181982 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama': 'indicador'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop_rama  162 non-null    float64
#  
#  |    | indicador                  |   anio |   prop_rama |
#  |---:|:---------------------------|-------:|------------:|
#  |  0 | Investigación y desarrollo |   1996 |   0.0181982 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_rama': 'valor'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Ss. publicidad', new_value='Publicidad')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Ss. Arquitectura', new_value='Arquitectura')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Ss. Jurídicos y de contabilidad', new_value='Jurídicos y contables')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Investigación y desarrollo': 0, 'Publicidad': 1, 'Otras': 5, 'Arquitectura': 2, 'Jurídicos y contables': 3, 'SSI': 4}, prefix=['anio'], suffix=[])
#  Index: 162 entries, 0 to 53
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | indicador                  |   anio |   valor |
#  |---:|:---------------------------|-------:|--------:|
#  |  0 | Investigación y desarrollo |   1996 | 1.81982 |
#  
#  ------------------------------
#  