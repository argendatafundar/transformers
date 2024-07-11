from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'rubro': 'indicador', 'precio_relativo': 'valor'}),
	sort_values(how='ascending', by=['anio', 'indicador']),
	drop_na(cols=['valor']),
	query(condition='valor > 0')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 780 entries, 0 to 779
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             780 non-null    int64  
#   1   rubro            780 non-null    object 
#   2   precio_relativo  780 non-null    float64
#  
#  |    |   anio | rubro               |   precio_relativo |
#  |---:|-------:|:--------------------|------------------:|
#  |  0 |   2006 | Alimentos y bebidas |           97.3951 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rubro': 'indicador', 'precio_relativo': 'valor'})
#  RangeIndex: 780 entries, 0 to 779
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       780 non-null    int64  
#   1   indicador  780 non-null    object 
#   2   valor      780 non-null    float64
#  
#  |    |   anio | indicador           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2006 | Alimentos y bebidas | 97.3951 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  RangeIndex: 780 entries, 0 to 779
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       780 non-null    int64  
#   1   indicador  780 non-null    object 
#   2   valor      780 non-null    float64
#  
#  |    |   anio | indicador           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1947 | Alimentos y bebidas | 92.3782 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  RangeIndex: 780 entries, 0 to 779
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       780 non-null    int64  
#   1   indicador  780 non-null    object 
#   2   valor      780 non-null    float64
#  
#  |    |   anio | indicador           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1947 | Alimentos y bebidas | 92.3782 |
#  
#  ------------------------------
#  
#  query(condition='valor > 0')
#  Index: 672 entries, 0 to 779
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       672 non-null    int64  
#   1   indicador  672 non-null    object 
#   2   valor      672 non-null    float64
#  
#  |    |   anio | indicador           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1947 | Alimentos y bebidas | 92.3782 |
#  
#  ------------------------------
#  