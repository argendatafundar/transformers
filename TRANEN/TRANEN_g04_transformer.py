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
	rename_cols(map={'valor_en_porcentaje': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4543 entries, 0 to 4542
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 4543 non-null   int64  
#   1   iso3                 4543 non-null   object 
#   2   valor_en_porcentaje  3529 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_porcentaje |
#  |---:|-------:|:-------|----------------------:|
#  |  0 |   1965 | GBR    |               2.44292 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 4543 entries, 0 to 4542
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 4543 non-null   int64  
#   1   iso3                 4543 non-null   object 
#   2   valor_en_porcentaje  3529 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_porcentaje |
#  |---:|-------:|:-------|----------------------:|
#  |  0 |   1965 | GBR    |               2.44292 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_porcentaje': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 4543 entries, 0 to 4542
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4543 non-null   int64  
#   1   geocodigo  4543 non-null   object 
#   2   valor      3529 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         | 2.44292 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 3529 entries, 0 to 4538
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3529 non-null   int64  
#   1   geocodigo  3529 non-null   object 
#   2   valor      3529 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         | 2.44292 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  Index: 3529 entries, 590 to 1061
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3529 non-null   int64  
#   1   geocodigo  3529 non-null   object 
#   2   valor      3529 non-null   float64
#  
#  |     |   anio | geocodigo   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 590 |   1965 | ARG         | 1.10269 |
#  
#  ------------------------------
#  