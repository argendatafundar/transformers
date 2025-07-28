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
	rename_cols(map={'sector': 'indicador', 'balanza': 'valor'}),
	sort_values(how='ascending', by=['anio', 'valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     102 non-null    int64  
#   1   sector   102 non-null    object 
#   2   balanza  102 non-null    float64
#  
#  |    |   anio | sector                |   balanza |
#  |---:|-------:|:----------------------|----------:|
#  |  0 |   2006 | Propiedad intelectual |  -814.312 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'balanza': 'valor'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador             |    valor |
#  |---:|-------:|:----------------------|---------:|
#  |  0 |   2006 | Propiedad intelectual | -814.312 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'valor'])
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador             |    valor |
#  |---:|-------:|:----------------------|---------:|
#  |  0 |   2006 | Propiedad intelectual | -814.312 |
#  
#  ------------------------------
#  