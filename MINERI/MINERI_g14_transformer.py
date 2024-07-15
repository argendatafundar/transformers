from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'}),
	replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'}),
	sort_values(how='ascending', by=['categoria', 'indicador'])
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
#  rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | asalariados_registrados |   15.91 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | Asalariados registrados |   15.91 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria', 'indicador'])
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria            | indicador                  |   valor |
#  |---:|:---------------------|:---------------------------|--------:|
#  |  0 | Act. Administrativas | Asalariados no registrados |   17.88 |
#  
#  ------------------------------
#  