from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys='anio', value_name='valor', var_name='categoria'),
	drop_na(cols=['valor']),
	replace_value(col='categoria', curr_value='petroleo_mineria', new_value='Petróleo y minería'),
	replace_value(col='categoria', curr_value='mineria', new_value='Minería')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              88 non-null     int64  
#   1   petroleo_mineria  88 non-null     float64
#   2   mineria           19 non-null     float64
#  
#  |    |   anio |   petroleo_mineria |   mineria |
#  |---:|-------:|-------------------:|----------:|
#  |  0 |   1935 |           0.380483 |       nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys='anio', value_name='valor', var_name='categoria')
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       176 non-null    int64  
#   1   categoria  176 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    |   anio | categoria        |    valor |
#  |---:|-------:|:-----------------|---------:|
#  |  0 |   1935 | petroleo_mineria | 0.380483 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 107 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       107 non-null    int64  
#   1   categoria  107 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    |   anio | categoria        |    valor |
#  |---:|-------:|:-----------------|---------:|
#  |  0 |   1935 | petroleo_mineria | 0.380483 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='petroleo_mineria', new_value='Petróleo y minería')
#  Index: 107 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       107 non-null    int64  
#   1   categoria  107 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    |   anio | categoria          |    valor |
#  |---:|-------:|:-------------------|---------:|
#  |  0 |   1935 | Petróleo y minería | 0.380483 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='mineria', new_value='Minería')
#  Index: 107 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       107 non-null    int64  
#   1   categoria  107 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    |   anio | categoria          |    valor |
#  |---:|-------:|:-------------------|---------:|
#  |  0 |   1935 | Petróleo y minería | 0.380483 |
#  
#  ------------------------------
#  