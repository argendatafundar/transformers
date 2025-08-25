from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='agua', new_value='Agua'),
	replace_value(col='categoria', curr_value='sanidad', new_value='Saneamiento baño'),
	replace_value(col='categoria', curr_value='cloacas', new_value='Cloacas')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               43 non-null     int64  
#   1   genero_cod         43 non-null     int64  
#   2   genero_desc        43 non-null     object 
#   3   hs_trabajadas_sem  43 non-null     float64
#  
#  |    |   anio |   genero_cod | genero_desc   |   hs_trabajadas_sem |
#  |---:|-------:|-------------:|:--------------|--------------------:|
#  |  0 |   2003 |            0 | Mujeres       |                  98 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               43 non-null     int64  
#   1   genero_cod         43 non-null     int64  
#   2   genero_desc        43 non-null     object 
#   3   hs_trabajadas_sem  43 non-null     float64
#  
#  |    |   anio |   genero_cod | genero_desc   |   hs_trabajadas_sem |
#  |---:|-------:|-------------:|:--------------|--------------------:|
#  |  0 |   2003 |            0 | Mujeres       |                  98 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='agua', new_value='Agua')
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               43 non-null     int64  
#   1   genero_cod         43 non-null     int64  
#   2   genero_desc        43 non-null     object 
#   3   hs_trabajadas_sem  43 non-null     float64
#  
#  |    |   anio |   genero_cod | genero_desc   |   hs_trabajadas_sem |
#  |---:|-------:|-------------:|:--------------|--------------------:|
#  |  0 |   2003 |            0 | Mujeres       |                  98 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='sanidad', new_value='Saneamiento baño')
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               43 non-null     int64  
#   1   genero_cod         43 non-null     int64  
#   2   genero_desc        43 non-null     object 
#   3   hs_trabajadas_sem  43 non-null     float64
#  
#  |    |   anio |   genero_cod | genero_desc   |   hs_trabajadas_sem |
#  |---:|-------:|-------------:|:--------------|--------------------:|
#  |  0 |   2003 |            0 | Mujeres       |                  98 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='cloacas', new_value='Cloacas')
#  RangeIndex: 43 entries, 0 to 42
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               43 non-null     int64  
#   1   genero_cod         43 non-null     int64  
#   2   genero_desc        43 non-null     object 
#   3   hs_trabajadas_sem  43 non-null     float64
#  
#  |    |   anio |   genero_cod | genero_desc   |   hs_trabajadas_sem |
#  |---:|-------:|-------------:|:--------------|--------------------:|
#  |  0 |   2003 |            0 | Mujeres       |                  98 |
#  
#  ------------------------------
#  