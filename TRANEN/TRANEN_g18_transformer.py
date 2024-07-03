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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7344 entries, 0 to 7343
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7344 non-null   int64  
#   1   iso3                   7344 non-null   object 
#   2   valor_en_gco2_por_kwh  5342 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   1990 | GBR    |                  705.02 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 7344 entries, 0 to 7343
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7344 non-null   int64  
#   1   iso3                   7344 non-null   object 
#   2   valor_en_gco2_por_kwh  5342 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   1990 | GBR    |                  705.02 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 7344 entries, 0 to 7343
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   7344 non-null   int64  
#   1   iso3                   7344 non-null   object 
#   2   valor_en_gco2_por_kwh  5342 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|
#  |  0 |   1990 | GBR    |                  705.02 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_gco2_por_kwh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 7344 entries, 0 to 7343
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7344 non-null   int64  
#   1   geocodigo  7344 non-null   object 
#   2   valor      5342 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1990 | GBR         |  705.02 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 5342 entries, 0 to 7342
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5342 non-null   int64  
#   1   geocodigo  5342 non-null   object 
#   2   valor      5342 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1990 | GBR         |  705.02 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  Index: 5342 entries, 340 to 2855
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5342 non-null   int64  
#   1   geocodigo  5342 non-null   object 
#   2   valor      5342 non-null   float64
#  
#  |     |   anio | geocodigo   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 340 |   1990 | AUT         | 249.848 |
#  
#  ------------------------------
#  