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
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'rama': 'indicador'}),
	rename_cols(map={'prop_rama': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value='Ss. publicidad', new_value='Ss. Publicidad', mapping=None)
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
#  replace_value(col='indicador', curr_value='Ss. publicidad', new_value='Ss. Publicidad', mapping=None)
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