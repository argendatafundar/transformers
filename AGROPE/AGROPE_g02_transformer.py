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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'pbi_agro_pcons': 'valor'}),
	sort_values(how='ascending', by='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype
#  ---  ------          --------------  -----
#   0   anio            148 non-null    int64
#   1   pbi_agro_pcons  148 non-null    int64
#  
#  |    |   anio |   pbi_agro_pcons |
#  |---:|-------:|-----------------:|
#  |  0 |   1875 |              971 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pbi_agro_pcons': 'valor'})
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype
#  ---  ------  --------------  -----
#   0   anio    148 non-null    int64
#   1   valor   148 non-null    int64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1875 |     971 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by='anio')
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype
#  ---  ------  --------------  -----
#   0   anio    148 non-null    int64
#   1   valor   148 non-null    int64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1875 |     971 |
#  
#  ------------------------------
#  