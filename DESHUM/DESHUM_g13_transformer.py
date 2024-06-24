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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def to_upper(df: DataFrame, col:str):
    df[col] = df[col].str.upper()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD'),
	replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD'),
	replace_value(col='iso3', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD'),
	replace_value(col='iso3', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD'),
	replace_value(col='iso3', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD'),
	replace_value(col='iso3', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS'),
	replace_value(col='iso3', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP'),
	replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD'),
	replace_value(col='iso3', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA'),
	replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC'),
	replace_value(col='iso3', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA'),
	replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA'),
	drop_col(col=['pais_nombre', 'es_agregacion', 'anio'], axis=1),
	rename_cols(map={'continente_fundar': 'grupo', 'iso3': 'geocodigo'}),
	wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador'),
	to_upper(col='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5095 entries, 0 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               5095 non-null   object 
#   1   pais_nombre        5095 non-null   object 
#   2   continente_fundar  4734 non-null   object 
#   3   es_agregacion      4734 non-null   float64
#   4   anio               5095 non-null   int64  
#   5   idh                5095 non-null   float64
#   6   dif_idh_idhp       5095 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 | 0.284 |        1.05634 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   pais_nombre        165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'es_agregacion', 'anio'], axis=1)
#  Index: 165 entries, 32 to 5094
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               165 non-null    object 
#   1   continente_fundar  154 non-null    object 
#   2   idh                165 non-null    float64
#   3   dif_idh_idhp       165 non-null    float64
#  
#  |    | iso3   | continente_fundar   |   idh |   dif_idh_idhp |
#  |---:|:-------|:--------------------|------:|---------------:|
#  | 32 | AFG    | Asia                | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'grupo', 'iso3': 'geocodigo'})
#  Index: 165 entries, 32 to 5094
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     165 non-null    object 
#   1   grupo         154 non-null    object 
#   2   idh           165 non-null    float64
#   3   dif_idh_idhp  165 non-null    float64
#  
#  |    | geocodigo   | grupo   |   idh |   dif_idh_idhp |
#  |---:|:------------|:--------|------:|---------------:|
#  | 32 | AFG         | Asia    | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      308 non-null    object 
#   1   geocodigo  330 non-null    object 
#   2   indicador  330 non-null    object 
#   3   valor      330 non-null    float64
#  
#  |    | grupo   | geocodigo   | indicador   |   valor |
#  |---:|:--------|:------------|:------------|--------:|
#  |  0 | Asia    | AFG         | IDH         |   0.462 |
#  
#  ------------------------------
#  
#  to_upper(col='indicador')
#  RangeIndex: 330 entries, 0 to 329
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      308 non-null    object 
#   1   geocodigo  330 non-null    object 
#   2   indicador  330 non-null    object 
#   3   valor      330 non-null    float64
#  
#  |    | grupo   | geocodigo   | indicador   |   valor |
#  |---:|:--------|:------------|:------------|--------:|
#  |  0 | Asia    | AFG         | IDH         |   0.462 |
#  
#  ------------------------------
#  