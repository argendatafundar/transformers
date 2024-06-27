from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'producto_exportacion': 'categoria', 'ano': 'anio'}),
	replace_value(col='categoria', curr_value='cereales', new_value='Cereales'),
	replace_value(col='categoria', curr_value='semillas_frutos_oleaginosos', new_value='Semillas y frutos oleaginosos'),
	replace_value(col='categoria', curr_value='grasas_aceites', new_value='Grasas y aceites'),
	replace_value(col='categoria', curr_value='residuos_alimenticios', new_value='Residuos alimenticios'),
	replace_value(col='categoria', curr_value='carnes_preparados', new_value='Carnes y preparados')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  185 non-null    int64  
#   1   producto_exportacion  185 non-null    object 
#   2   valor                 185 non-null    float64
#  
#  |    |   anio | producto_exportacion   |   valor |
#  |---:|-------:|:-----------------------|--------:|
#  |  0 |   1986 | cereales               |    77.9 |
#  
#  ------------------------------
#  
#  rename_cols(map={'producto_exportacion': 'categoria', 'ano': 'anio'})
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | cereales    |    77.9 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='cereales', new_value='Cereales')
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Cereales    |    77.9 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='semillas_frutos_oleaginosos', new_value='Semillas y frutos oleaginosos')
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Cereales    |    77.9 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='grasas_aceites', new_value='Grasas y aceites')
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Cereales    |    77.9 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='residuos_alimenticios', new_value='Residuos alimenticios')
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Cereales    |    77.9 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='carnes_preparados', new_value='Carnes y preparados')
#  RangeIndex: 185 entries, 0 to 184
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       185 non-null    int64  
#   1   categoria  185 non-null    object 
#   2   valor      185 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1986 | Cereales    |    77.9 |
#  
#  ------------------------------
#  