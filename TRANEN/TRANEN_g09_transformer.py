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
replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'valor_en_gw': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3634 entries, 0 to 3633
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         3634 non-null   int64  
#   1   iso3         3634 non-null   object 
#   2   valor_en_gw  2603 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gw |
#  |---:|-------:|:-------|--------------:|
#  |  0 |   2012 | AFG    |        0.0001 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 3634 entries, 0 to 3633
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         3634 non-null   int64  
#   1   iso3         3634 non-null   object 
#   2   valor_en_gw  2603 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gw |
#  |---:|-------:|:-------|--------------:|
#  |  0 |   2012 | AFG    |        0.0001 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 3634 entries, 0 to 3633
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         3634 non-null   int64  
#   1   iso3         3634 non-null   object 
#   2   valor_en_gw  2603 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_gw |
#  |---:|-------:|:-------|--------------:|
#  |  0 |   2012 | AFG    |        0.0001 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_gw': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 3634 entries, 0 to 3633
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3634 non-null   int64  
#   1   geocodigo  3634 non-null   object 
#   2   valor      2603 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2012 | AFG         |  0.0001 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 2603 entries, 0 to 3633
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       2603 non-null   int64  
#   1   geocodigo  2603 non-null   object 
#   2   valor      2603 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2012 | AFG         |  0.0001 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  Index: 2603 entries, 80 to 3023
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       2603 non-null   int64  
#   1   geocodigo  2603 non-null   object 
#   2   valor      2603 non-null   float64
#  
#  |    |   anio | geocodigo   |    valor |
#  |---:|-------:|:------------|---------:|
#  | 80 |   2000 | ARG         | 0.014112 |
#  
#  ------------------------------
#  