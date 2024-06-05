from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	replace_value(col='iso3', curr_value='OWID_CZS', new_value='CSK'),
	replace_value(col='iso3', curr_value='OWID_YGS', new_value='SER'),
	rename_cols(map={'valor_en_kwh_por_dolar': 'valor', 'iso3': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9860 entries, 0 to 9859
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9860 non-null   int64  
#   1   iso3                    9860 non-null   object 
#   2   valor_en_kwh_por_dolar  7832 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1980 | AFG    |                  0.50821 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 9860 entries, 0 to 9859
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9860 non-null   int64  
#   1   iso3                    9860 non-null   object 
#   2   valor_en_kwh_por_dolar  7832 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1980 | AFG    |                  0.50821 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_CZS', new_value='CSK')
#  RangeIndex: 9860 entries, 0 to 9859
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9860 non-null   int64  
#   1   iso3                    9860 non-null   object 
#   2   valor_en_kwh_por_dolar  7832 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1980 | AFG    |                  0.50821 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_YGS', new_value='SER')
#  RangeIndex: 9860 entries, 0 to 9859
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9860 non-null   int64  
#   1   iso3                    9860 non-null   object 
#   2   valor_en_kwh_por_dolar  7832 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1980 | AFG    |                  0.50821 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_kwh_por_dolar': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 9860 entries, 0 to 9859
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       9860 non-null   int64  
#   1   geocodigo  9860 non-null   object 
#   2   valor      7832 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1980 | AFG         | 0.50821 |
#  
#  ------------------------------
#  