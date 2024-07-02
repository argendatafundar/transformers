from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'puestos_per_capita': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12810 entries, 0 to 12809
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   iso3                12810 non-null  object 
#   1   anio                12810 non-null  int64  
#   2   puestos_per_capita  9529 non-null   float64
#  
#  |    | iso3   |   anio |   puestos_per_capita |
#  |---:|:-------|-------:|---------------------:|
#  |  0 | ABW    |   1950 |                  nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'puestos_per_capita': 'valor'})
#  RangeIndex: 12810 entries, 0 to 12809
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  12810 non-null  object 
#   1   anio       12810 non-null  int64  
#   2   valor      9529 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1950 |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 9529 entries, 41 to 12809
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9529 non-null   object 
#   1   anio       9529 non-null   int64  
#   2   valor      9529 non-null   float64
#  
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  | 41 | ABW         |   1991 | 0.451859 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 9529 entries, 0 to 9528
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9529 non-null   object 
#   1   anio       9529 non-null   int64  
#   2   valor      9529 non-null   float64
#  
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | ARG         |   1950 | 0.386666 |
#  
#  ------------------------------
#  