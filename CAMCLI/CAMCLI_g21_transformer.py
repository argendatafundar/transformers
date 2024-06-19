from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def medias_anuales(df, date_col='fecha', year_col='anio', cols_valores=None):
    import pandas as pd
    df[date_col] = pd.to_datetime(df[date_col])

    df[year_col] = df[date_col].dt.year

    if cols_valores is None:
        cols_valores = [col for col in df.columns if col not in [date_col, year_col] and df[col].dtype in ['float64', 'int64']]

    yearly_means = df.groupby(year_col)[cols_valores].mean().reset_index()
    yearly_means[date_col] = pd.to_datetime(yearly_means[year_col].astype(str) + '-01-01')
    result_df = yearly_means[[date_col, year_col] + cols_valores]

    return result_df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
medias_anuales(date_col='fecha', year_col='anio', cols_valores=None),
	drop_col(col='fecha', axis=1),
	wide_to_long(primary_keys='anio', value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 10808 entries, 0 to 10807
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype         
#  ---  ------                                     --------------  -----         
#   0   fecha                                      10808 non-null  datetime64[ns]
#   1   altura_nivel_mar_corr_tpac_drift           10808 non-null  float64       
#   2   altura_nivel_mar_filtrada_corr_tpac_drift  10808 non-null  float64       
#   3   anio                                       10808 non-null  int32         
#  
#  |    | fecha               |   altura_nivel_mar_corr_tpac_drift |   altura_nivel_mar_filtrada_corr_tpac_drift |   anio |
#  |---:|:--------------------|-----------------------------------:|--------------------------------------------:|-------:|
#  |  0 | 1993-01-01 00:00:00 |                         0.00412627 |                                  0.00362401 |   1993 |
#  
#  ------------------------------
#  
#  medias_anuales(date_col='fecha', year_col='anio', cols_valores=None)
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 4 columns):
#   #   Column                                     Non-Null Count  Dtype         
#  ---  ------                                     --------------  -----         
#   0   fecha                                      30 non-null     datetime64[ns]
#   1   anio                                       30 non-null     int32         
#   2   altura_nivel_mar_corr_tpac_drift           30 non-null     float64       
#   3   altura_nivel_mar_filtrada_corr_tpac_drift  30 non-null     float64       
#  
#  |    | fecha               |   anio |   altura_nivel_mar_corr_tpac_drift |   altura_nivel_mar_filtrada_corr_tpac_drift |
#  |---:|:--------------------|-------:|-----------------------------------:|--------------------------------------------:|
#  |  0 | 1993-01-01 00:00:00 |   1993 |                         0.00712589 |                                  0.00715534 |
#  
#  ------------------------------
#  
#  drop_col(col='fecha', axis=1)
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 3 columns):
#   #   Column                                     Non-Null Count  Dtype  
#  ---  ------                                     --------------  -----  
#   0   anio                                       30 non-null     int32  
#   1   altura_nivel_mar_corr_tpac_drift           30 non-null     float64
#   2   altura_nivel_mar_filtrada_corr_tpac_drift  30 non-null     float64
#  
#  |    |   anio |   altura_nivel_mar_corr_tpac_drift |   altura_nivel_mar_filtrada_corr_tpac_drift |
#  |---:|-------:|-----------------------------------:|--------------------------------------------:|
#  |  0 |   1993 |                         0.00712589 |                                  0.00715534 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys='anio', value_name='valor', var_name='indicador')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       60 non-null     int32  
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1993 | altura_nivel_mar_corr_tpac_drift | 0.00712589 |
#  
#  ------------------------------
#  