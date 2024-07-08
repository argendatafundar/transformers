from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='tasa_inflacion', axis=1),
	rename_columns(tasa_inflacion_valores_positivos='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   anio                              89 non-null     int64  
#   1   tasa_inflacion                    88 non-null     float64
#   2   tasa_inflacion_valores_positivos  80 non-null     float64
#  
#  |    |   anio |   tasa_inflacion |   tasa_inflacion_valores_positivos |
#  |---:|-------:|-----------------:|-----------------------------------:|
#  |  0 |   1935 |          14.0881 |                            14.0881 |
#  
#  ------------------------------
#  
#  drop_col(col='tasa_inflacion', axis=1)
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   anio                              89 non-null     int64  
#   1   tasa_inflacion_valores_positivos  80 non-null     float64
#  
#  |    |   anio |   tasa_inflacion_valores_positivos |
#  |---:|-------:|-----------------------------------:|
#  |  0 |   1935 |                            14.0881 |
#  
#  ------------------------------
#  
#  rename_columns(tasa_inflacion_valores_positivos='valor')
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    89 non-null     int64  
#   1   valor   80 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1935 | 14.0881 |
#  
#  ------------------------------
#  
