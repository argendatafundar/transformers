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
	rename_cols(map={'valor_en_twh': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12685 entries, 0 to 12684
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12685 non-null  int64  
#   1   iso3          12685 non-null  object 
#   2   valor_en_twh  8154 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1965 | GBR    |         15.135 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 12685 entries, 0 to 12684
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12685 non-null  int64  
#   1   iso3          12685 non-null  object 
#   2   valor_en_twh  8154 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1965 | GBR    |         15.135 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 12685 entries, 0 to 12684
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          12685 non-null  int64  
#   1   iso3          12685 non-null  object 
#   2   valor_en_twh  8154 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_twh |
#  |---:|-------:|:-------|---------------:|
#  |  0 |   1965 | GBR    |         15.135 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 12685 entries, 0 to 12684
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12685 non-null  int64  
#   1   geocodigo  12685 non-null  object 
#   2   valor      8154 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         |  15.135 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 8154 entries, 0 to 12684
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8154 non-null   int64  
#   1   geocodigo  8154 non-null   object 
#   2   valor      8154 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         |  15.135 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  Index: 8154 entries, 826 to 5368
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8154 non-null   int64  
#   1   geocodigo  8154 non-null   object 
#   2   valor      8154 non-null   float64
#  
#  |     |   anio | geocodigo   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 826 |   1965 | AGO         |       0 |
#  
#  ------------------------------
#  