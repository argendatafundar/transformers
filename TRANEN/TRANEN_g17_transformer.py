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

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	replace_value(col='iso3', curr_value='OWID_CZS', new_value='CSK'),
	replace_value(col='iso3', curr_value='OWID_YGS', new_value='SER'),
	rename_cols(map={'valor_en_kwh_por_dolar': 'valor', 'iso3': 'geocodigo'}),
	drop_na(cols='valor'),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition='anio >= 2000')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9802 entries, 0 to 9801
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9802 non-null   int64  
#   1   iso3                    9802 non-null   object 
#   2   valor_en_kwh_por_dolar  7789 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1965 | GBR    |                  2.74151 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 9802 entries, 0 to 9801
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9802 non-null   int64  
#   1   iso3                    9802 non-null   object 
#   2   valor_en_kwh_por_dolar  7789 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1965 | GBR    |                  2.74151 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_CZS', new_value='CSK')
#  RangeIndex: 9802 entries, 0 to 9801
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9802 non-null   int64  
#   1   iso3                    9802 non-null   object 
#   2   valor_en_kwh_por_dolar  7789 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1965 | GBR    |                  2.74151 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_YGS', new_value='SER')
#  RangeIndex: 9802 entries, 0 to 9801
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    9802 non-null   int64  
#   1   iso3                    9802 non-null   object 
#   2   valor_en_kwh_por_dolar  7789 non-null   float64
#  
#  |    |   anio | iso3   |   valor_en_kwh_por_dolar |
#  |---:|-------:|:-------|-------------------------:|
#  |  0 |   1965 | GBR    |                  2.74151 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_kwh_por_dolar': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 9802 entries, 0 to 9801
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       9802 non-null   int64  
#   1   geocodigo  9802 non-null   object 
#   2   valor      7789 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         | 2.74151 |
#  
#  ------------------------------
#  
#  drop_na(cols='valor')
#  Index: 7789 entries, 0 to 9800
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7789 non-null   int64  
#   1   geocodigo  7789 non-null   object 
#   2   valor      7789 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1965 | GBR         | 2.74151 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 7789 entries, 0 to 7788
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7789 non-null   int64  
#   1   geocodigo  7789 non-null   object 
#   2   valor      7789 non-null   float64
#  
#  |    |   anio | geocodigo   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1965 | ARE         | 0.142491 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 2000')
#  Index: 3706 entries, 4083 to 7788
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3706 non-null   int64  
#   1   geocodigo  3706 non-null   object 
#   2   valor      3706 non-null   float64
#  
#  |      |   anio | geocodigo   |   valor |
#  |-----:|-------:|:------------|--------:|
#  | 4083 |   2000 | AFG         | 0.52408 |
#  
#  ------------------------------
#  