from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
def df_merge_geonomenclador(df: DataFrame, left:str = 'iso3', right:str = 'geocodigo', how:str='left'):
    df =  df.merge(geonomenclador, left_on = left, right_on = right, how = how)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD'),
	replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC'),
	replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA'),
	df_merge_geonomenclador(left='iso3', right='geocodigo', how='left'),
	drop_col(col=['geocodigo', 'iso3', 'country', 'name_short', 'iso_2', 'anio'], axis=1),
	rename_cols(map={'name_long': 'categoria', 'sexo': 'indicador', 'idh': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13596 entries, 0 to 13595
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     13596 non-null  object 
#   1   country  13596 non-null  object 
#   2   anio     13596 non-null  int64  
#   3   sexo     13596 non-null  object 
#   4   idh      10028 non-null  float64
#  
#  |    | iso3   | country     |   anio | sexo    |   idh |
#  |---:|:-------|:------------|-------:|:--------|------:|
#  |  0 | AFG    | Afghanistan |   1990 | Varones |   nan |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 412 entries, 32 to 13595
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     412 non-null    object 
#   1   country  412 non-null    object 
#   2   anio     412 non-null    int64  
#   3   sexo     412 non-null    object 
#   4   idh      386 non-null    float64
#  
#  |    | iso3   | country     |   anio | sexo    |      idh |
#  |---:|:-------|:------------|-------:|:--------|---------:|
#  | 32 | AFG    | Afghanistan |   2022 | Varones | 0.534145 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  Index: 412 entries, 32 to 13595
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     412 non-null    object 
#   1   country  412 non-null    object 
#   2   anio     412 non-null    int64  
#   3   sexo     412 non-null    object 
#   4   idh      386 non-null    float64
#  
#  |    | iso3   | country     |   anio | sexo    |      idh |
#  |---:|:-------|:------------|-------:|:--------|---------:|
#  | 32 | AFG    | Afghanistan |   2022 | Varones | 0.534145 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  Index: 412 entries, 32 to 13595
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     412 non-null    object 
#   1   country  412 non-null    object 
#   2   anio     412 non-null    int64  
#   3   sexo     412 non-null    object 
#   4   idh      386 non-null    float64
#  
#  |    | iso3   | country     |   anio | sexo    |      idh |
#  |---:|:-------|:------------|-------:|:--------|---------:|
#  | 32 | AFG    | Afghanistan |   2022 | Varones | 0.534145 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  Index: 412 entries, 32 to 13595
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   iso3     412 non-null    object 
#   1   country  412 non-null    object 
#   2   anio     412 non-null    int64  
#   3   sexo     412 non-null    object 
#   4   idh      386 non-null    float64
#  
#  |    | iso3   | country     |   anio | sexo    |      idh |
#  |---:|:-------|:------------|-------:|:--------|---------:|
#  | 32 | AFG    | Afghanistan |   2022 | Varones | 0.534145 |
#  
#  ------------------------------
#  
#  df_merge_geonomenclador(left='iso3', right='geocodigo', how='left')
#  RangeIndex: 412 entries, 0 to 411
#  Data columns (total 9 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   iso3        412 non-null    object 
#   1   country     412 non-null    object 
#   2   anio        412 non-null    int64  
#   3   sexo        412 non-null    object 
#   4   idh         386 non-null    float64
#   5   geocodigo   396 non-null    object 
#   6   name_long   396 non-null    object 
#   7   name_short  396 non-null    object 
#   8   iso_2       388 non-null    object 
#  
#  |    | iso3   | country     |   anio | sexo    |      idh | geocodigo   | name_long   | name_short   | iso_2   |
#  |---:|:-------|:------------|-------:|:--------|---------:|:------------|:------------|:-------------|:--------|
#  |  0 | AFG    | Afghanistan |   2022 | Varones | 0.534145 | AFG         | Afganistán  | Afganistán   | AF      |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigo', 'iso3', 'country', 'name_short', 'iso_2', 'anio'], axis=1)
#  RangeIndex: 412 entries, 0 to 411
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   sexo       412 non-null    object 
#   1   idh        386 non-null    float64
#   2   name_long  396 non-null    object 
#  
#  |    | sexo    |      idh | name_long   |
#  |---:|:--------|---------:|:------------|
#  |  0 | Varones | 0.534145 | Afganistán  |
#  
#  ------------------------------
#  
#  rename_cols(map={'name_long': 'categoria', 'sexo': 'indicador', 'idh': 'valor'})
#  RangeIndex: 412 entries, 0 to 411
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  412 non-null    object 
#   1   valor      386 non-null    float64
#   2   categoria  396 non-null    object 
#  
#  |    | indicador   |    valor | categoria   |
#  |---:|:------------|---------:|:------------|
#  |  0 | Varones     | 0.534145 | Afganistán  |
#  
#  ------------------------------
#  