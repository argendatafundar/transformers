from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df, args, how='ascending'):
    if how == 'ascending':
        ascending = [True] * len(args)
    elif how == 'descending':
        ascending = [False] * len(args)
    else:
        raise ValueError("Invalid value for 'how'. Use 'ascending' or 'descending'.")

    sorted_df = df.sort_values(by=list(args), ascending=ascending)
    return sorted_df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'cuenta': 'indicador', 'participacion_pbi': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(args=['anio'], how='ascending')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               76 non-null     int64  
#   1   cuenta             76 non-null     object 
#   2   participacion_pbi  76 non-null     float64
#  
#  |    |   anio | cuenta      |   participacion_pbi |
#  |---:|-------:|:------------|--------------------:|
#  |  0 |   2004 | Agricultura |                0.05 |
#  
#  ------------------------------
#  
#  rename_cols(map={'cuenta': 'indicador', 'participacion_pbi': 'valor'})
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  
#  sort_values(args=['anio'], how='ascending')
#  Index: 76 entries, 0 to 75
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       76 non-null     int64  
#   1   indicador  76 non-null     object 
#   2   valor      76 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2004 | Agricultura |       5 |
#  
#  ------------------------------
#  