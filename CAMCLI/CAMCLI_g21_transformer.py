from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def datetime_to_year(df, col: str):
    df[col] = pd.to_datetime(df[col]).dt.year
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	rename_cols(map={'fecha': 'anio'}),
	datetime_to_year(col='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10808 entries, 0 to 10807
#  Data columns (total 3 columns):
#   #   Column                                     Non-Null Count  Dtype  
#  ---  ------                                     --------------  -----  
#   0   fecha                                      10808 non-null  object 
#   1   altura_nivel_mar_corr_tpac_drift           10808 non-null  float64
#   2   altura_nivel_mar_filtrada_corr_tpac_drift  10808 non-null  float64
#  
#  |    | fecha      |   altura_nivel_mar_corr_tpac_drift |   altura_nivel_mar_filtrada_corr_tpac_drift |
#  |---:|:-----------|-----------------------------------:|--------------------------------------------:|
#  |  0 | 1993-01-01 |                         0.00412627 |                                  0.00362401 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  RangeIndex: 21616 entries, 0 to 21615
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   fecha      21616 non-null  object 
#   1   indicador  21616 non-null  object 
#   2   valor      21616 non-null  float64
#  
#  |    | fecha      | indicador                        |      valor |
#  |---:|:-----------|:---------------------------------|-----------:|
#  |  0 | 1993-01-01 | altura_nivel_mar_corr_tpac_drift | 0.00412627 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio'})
#  RangeIndex: 21616 entries, 0 to 21615
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21616 non-null  int32  
#   1   indicador  21616 non-null  object 
#   2   valor      21616 non-null  float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1993 | altura_nivel_mar_corr_tpac_drift | 0.00412627 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  RangeIndex: 21616 entries, 0 to 21615
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21616 non-null  int32  
#   1   indicador  21616 non-null  object 
#   2   valor      21616 non-null  float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1993 | altura_nivel_mar_corr_tpac_drift | 0.00412627 |
#  
#  ------------------------------
#  