from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
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
	rename_cols(map={'iso3': 'geocodigo', 'inb_pc': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   inb_pc             6659 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   inb_pc |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|---------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 |  3115.67 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'inb_pc': 'valor'})
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          6659 non-null   object 
#   1   pais_nombre        6659 non-null   object 
#   2   continente_fundar  6296 non-null   object 
#   3   es_agregacion      6296 non-null   float64
#   4   anio               6659 non-null   int64  
#   5   valor              6659 non-null   float64
#  
#  |    | geocodigo   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------|:--------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG         | Afganistán    | Asia                |               0 |   1990 | 3115.67 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6659 entries, 0 to 6658
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6659 non-null   object 
#   1   anio       6659 non-null   int64  
#   2   valor      6659 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1990 | 3115.67 |
#  
#  ------------------------------
#  