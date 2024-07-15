from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df: DataFrame, index: list, columns: str, values: str):
    return df.pivot_table(index=index, columns=columns, values=values).reset_index()

@transformer.convert
def rank_col(df: DataFrame, col:str, rank_col:str, ascending: bool):
    df[rank_col] = df[col].rank(ascending=ascending)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
long_to_wide(index='rama_actividad', columns='categoria_ocupacional', values='porcentaje_sobre_total_rama'),
	rank_col(col='asalariados_registrados', rank_col='rank', ascending=False),
	wide_to_long(primary_keys=['rama_actividad', 'rank'], value_name='valor', var_name='indicador'),
	sort_values(how='ascending', by='rank'),
	rename_cols(map={'rama_actividad': 'categoria'}),
	replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'}),
	drop_col(col='rank', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   rama_actividad               69 non-null     object 
#   1   categoria_ocupacional        69 non-null     object 
#   2   porcentaje_sobre_total_rama  69 non-null     float64
#  
#  |    | rama_actividad   | categoria_ocupacional   |   porcentaje_sobre_total_rama |
#  |---:|:-----------------|:------------------------|------------------------------:|
#  |  0 | Construcción     | asalariados_registrados |                         15.91 |
#  
#  ------------------------------
#  
#  long_to_wide(index='rama_actividad', columns='categoria_ocupacional', values='porcentaje_sobre_total_rama')
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   rama_actividad              23 non-null     object 
#   1   asalariados_no_registrados  23 non-null     float64
#   2   asalariados_registrados     23 non-null     float64
#   3   no_asalariados              23 non-null     float64
#   4   rank                        23 non-null     float64
#  
#  |    | rama_actividad       |   asalariados_no_registrados |   asalariados_registrados |   no_asalariados |   rank |
#  |---:|:---------------------|-----------------------------:|--------------------------:|-----------------:|-------:|
#  |  0 | Act. Administrativas |                        17.88 |                      56.4 |            25.72 |     11 |
#  
#  ------------------------------
#  
#  rank_col(col='asalariados_registrados', rank_col='rank', ascending=False)
#  RangeIndex: 23 entries, 0 to 22
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   rama_actividad              23 non-null     object 
#   1   asalariados_no_registrados  23 non-null     float64
#   2   asalariados_registrados     23 non-null     float64
#   3   no_asalariados              23 non-null     float64
#   4   rank                        23 non-null     float64
#  
#  |    | rama_actividad       |   asalariados_no_registrados |   asalariados_registrados |   no_asalariados |   rank |
#  |---:|:---------------------|-----------------------------:|--------------------------:|-----------------:|-------:|
#  |  0 | Act. Administrativas |                        17.88 |                      56.4 |            25.72 |     11 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['rama_actividad', 'rank'], value_name='valor', var_name='indicador')
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   rama_actividad  69 non-null     object 
#   1   rank            69 non-null     float64
#   2   indicador       69 non-null     object 
#   3   valor           69 non-null     float64
#  
#  |    | rama_actividad       |   rank | indicador                  |   valor |
#  |---:|:---------------------|-------:|:---------------------------|--------:|
#  |  0 | Act. Administrativas |     11 | asalariados_no_registrados |   17.88 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by='rank')
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   rama_actividad  69 non-null     object 
#   1   rank            69 non-null     float64
#   2   indicador       69 non-null     object 
#   3   valor           69 non-null     float64
#  
#  |    | rama_actividad     |   rank | indicador                  |   valor |
#  |---:|:-------------------|-------:|:---------------------------|--------:|
#  |  0 | Minería metalífera |      1 | asalariados_no_registrados |    5.28 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama_actividad': 'categoria'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   rank       69 non-null     float64
#   2   indicador  69 non-null     object 
#   3   valor      69 non-null     float64
#  
#  |    | categoria          |   rank | indicador                  |   valor |
#  |---:|:-------------------|-------:|:---------------------------|--------:|
#  |  0 | Minería metalífera |      1 | asalariados_no_registrados |    5.28 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   rank       69 non-null     float64
#   2   indicador  69 non-null     object 
#   3   valor      69 non-null     float64
#  
#  |    | categoria          |   rank | indicador                  |   valor |
#  |---:|:-------------------|-------:|:---------------------------|--------:|
#  |  0 | Minería metalífera |      1 | Asalariados no registrados |    5.28 |
#  
#  ------------------------------
#  
#  drop_col(col='rank', axis=1)
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria          | indicador                  |   valor |
#  |---:|:-------------------|:---------------------------|--------:|
#  |  0 | Minería metalífera | Asalariados no registrados |    5.28 |
#  
#  ------------------------------
#  