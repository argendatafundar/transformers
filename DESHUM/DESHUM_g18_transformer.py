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
	replace_value(col='iso3', curr_value='AHDI.EAS', new_value='DESHUM_AHDI.EAS'),
	replace_value(col='iso3', curr_value='AHDI.EEU', new_value='DESHUM_AHDI.EEU'),
	replace_value(col='iso3', curr_value='AHDI.LAC', new_value='DESHUM_AHDI.LAC'),
	replace_value(col='iso3', curr_value='AHDI.MEA', new_value='DESHUM_AHDI.MEA'),
	replace_value(col='iso3', curr_value='AHDI.NAF', new_value='DESHUM_AHDI.NAF'),
	replace_value(col='iso3', curr_value='AHDI.OECD', new_value='DESHUM_AHDI.OECD'),
	replace_value(col='iso3', curr_value='AHDI.ROW', new_value='DESHUM_AHDI.ROW'),
	replace_value(col='iso3', curr_value='AHDI.SAS', new_value='DESHUM_AHDI.SAS'),
	replace_value(col='iso3', curr_value='AHDI.SSA', new_value='DESHUM_AHDI.SSA'),
	replace_value(col='iso3', curr_value='AHDI.WEU', new_value='DESHUM_AHDI.WEU'),
	replace_value(col='iso3', curr_value='AHDI.WLD', new_value='DESHUM_AHDI.WLD'),
	replace_value(col='iso3', curr_value='AHDI.WOF', new_value='DESHUM_AHDI.WOF'),
	rename_cols(map={'idha': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['iso3_desc_fundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='ZZK.WORLD', new_value='WLD')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.EAS', new_value='DESHUM_AHDI.EAS')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.EEU', new_value='DESHUM_AHDI.EEU')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.LAC', new_value='DESHUM_AHDI.LAC')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.MEA', new_value='DESHUM_AHDI.MEA')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.NAF', new_value='DESHUM_AHDI.NAF')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.OECD', new_value='DESHUM_AHDI.OECD')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.ROW', new_value='DESHUM_AHDI.ROW')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.SAS', new_value='DESHUM_AHDI.SAS')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.SSA', new_value='DESHUM_AHDI.SSA')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.WEU', new_value='DESHUM_AHDI.WEU')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.WLD', new_value='DESHUM_AHDI.WLD')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='AHDI.WOF', new_value='DESHUM_AHDI.WOF')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   idha              3685 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   idha |
#  |---:|:-------|:-------------------|-------:|-------:|
#  |  0 | AFG    | Afganistán         |   1870 |    nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'idha': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         4176 non-null   object 
#   1   iso3_desc_fundar  4176 non-null   object 
#   2   anio              4176 non-null   int64  
#   3   valor             3685 non-null   float64
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:------------|:-------------------|-------:|--------:|
#  |  0 | AFG         | Afganistán         |   1870 |     nan |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3_desc_fundar'], axis=1)
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  4176 non-null   object 
#   1   anio       4176 non-null   int64  
#   2   valor      3685 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1870 |     nan |
#  
#  ------------------------------
#  