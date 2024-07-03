from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(rubro='categoria', precio_relativo='valor'),
	sort_values(how='ascending', by=['anio', 'categoria']),
	drop_na(cols=['valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             778 non-null    int64  
#   1   rubro            778 non-null    object 
#   2   precio_relativo  778 non-null    float64
#  
#  |    |   anio | rubro               |   precio_relativo |
#  |---:|-------:|:--------------------|------------------:|
#  |  0 |   2006 | Alimentos y bebidas |           97.3951 |
#  
#  ------------------------------
#  
#  rename_columns(rubro='categoria', precio_relativo='valor')
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       778 non-null    int64  
#   1   categoria  778 non-null    object 
#   2   valor      778 non-null    float64
#  
#  |    |   anio | categoria           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2006 | Alimentos y bebidas | 97.3951 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       778 non-null    int64  
#   1   categoria  778 non-null    object 
#   2   valor      778 non-null    float64
#  
#  |    |   anio | categoria           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1947 | Alimentos y bebidas | 92.3782 |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  RangeIndex: 778 entries, 0 to 777
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       778 non-null    int64  
#   1   categoria  778 non-null    object 
#   2   valor      778 non-null    float64
#  
#  |    |   anio | categoria           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1947 | Alimentos y bebidas | 92.3782 |
#  
#  ------------------------------
#  