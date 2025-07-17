from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'tipo_carne': 'indicador'}),
	ordenar_categorica(col1='indicador', order1=['Carne de cerdo', 'Carne aviar', 'Carne bovina']),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   tipo_carne  183 non-null    object 
#   1   anio        183 non-null    int64  
#   2   valor       183 non-null    float64
#  
#  |    | tipo_carne   |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_carne': 'indicador'})
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  183 non-null    category
#   1   anio       183 non-null    int64   
#   2   valor      183 non-null    float64 
#  
#  |    | indicador    |   anio |       valor |
#  |---:|:-------------|-------:|------------:|
#  |  0 | Carne bovina |   1961 | 2.14506e+06 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Carne de cerdo', 'Carne aviar', 'Carne bovina'])
#  Index: 183 entries, 182 to 0
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  183 non-null    category
#   1   anio       183 non-null    int64   
#   2   valor      183 non-null    float64 
#  
#  |     | indicador      |   anio |   valor |
#  |----:|:---------------|-------:|--------:|
#  | 182 | Carne de cerdo |   2021 |  695939 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  183 non-null    category
#   1   anio       183 non-null    int64   
#   2   valor      183 non-null    float64 
#  
#  |    | indicador      |   anio |   valor |
#  |---:|:---------------|-------:|--------:|
#  |  0 | Carne de cerdo |   1961 |  186932 |
#  
#  ------------------------------
#  