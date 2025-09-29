from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def to_pandas(df, dummy = True):
    import polars as pl
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='anomalia_temperatura_mar_relativ', new_value='Mar'),
	replace_value(col='indicador', curr_value='anomalia_temperatura_tierra_relativ', new_value='Tierra'),
	rename_cols(map={'indicador': 'categoria'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 175 entries, 0 to 174
#  Data columns (total 3 columns):
#   #   Column                               Non-Null Count  Dtype  
#  ---  ------                               --------------  -----  
#   0   anio                                 175 non-null    int64  
#   1   anomalia_temperatura_mar_relativ     175 non-null    float64
#   2   anomalia_temperatura_tierra_relativ  175 non-null    float64
#  
#  |    |   anio |   anomalia_temperatura_mar_relativ |   anomalia_temperatura_tierra_relativ |
#  |---:|-------:|-----------------------------------:|--------------------------------------:|
#  |  0 |   1850 |                         -0.0385351 |                             -0.326669 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 350 entries, 0 to 349
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       350 non-null    int64  
#   1   indicador  350 non-null    object 
#   2   valor      350 non-null    float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1850 | anomalia_temperatura_mar_relativ | -0.0385351 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='anomalia_temperatura_mar_relativ', new_value='Mar')
#  RangeIndex: 350 entries, 0 to 349
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       350 non-null    int64  
#   1   indicador  350 non-null    object 
#   2   valor      350 non-null    float64
#  
#  |    |   anio | indicador   |      valor |
#  |---:|-------:|:------------|-----------:|
#  |  0 |   1850 | Mar         | -0.0385351 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='anomalia_temperatura_tierra_relativ', new_value='Tierra')
#  RangeIndex: 350 entries, 0 to 349
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       350 non-null    int64  
#   1   indicador  350 non-null    object 
#   2   valor      350 non-null    float64
#  
#  |    |   anio | indicador   |      valor |
#  |---:|-------:|:------------|-----------:|
#  |  0 |   1850 | Mar         | -0.0385351 |
#  
#  ------------------------------
#  
#  rename_cols(map={'indicador': 'categoria'})
#  RangeIndex: 350 entries, 0 to 349
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       350 non-null    int64  
#   1   categoria  350 non-null    object 
#   2   valor      350 non-null    float64
#  
#  |    |   anio | categoria   |      valor |
#  |---:|-------:|:------------|-----------:|
#  |  0 |   1850 | Mar         | -0.0385351 |
#  
#  ------------------------------
#  