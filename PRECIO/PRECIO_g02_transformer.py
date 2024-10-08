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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'region': 'geocodigo', 'sector': 'indicador'}),
	replace_value(col='geocodigo', curr_value='Nacional', new_value='AR-NAC'),
	replace_value(col='geocodigo', curr_value='GBA', new_value='AR-GBA'),
	replace_value(col='geocodigo', curr_value='NOA', new_value='AR-NOA'),
	replace_value(col='geocodigo', curr_value='NEA', new_value='AR-NEA'),
	replace_value(col='geocodigo', curr_value='Pampeana', new_value='AR-PAM'),
	replace_value(col='geocodigo', curr_value='Cuyo', new_value='AR-CUY'),
	replace_value(col='geocodigo', curr_value='Patagonia', new_value='AR-PAT'),
	replace_value(col='indicador', curr_value='Alimentos y bebidas no alcoholicas', new_value='Alimentos y bebidas no alcohólicas'),
	replace_value(col='indicador', curr_value='Bebidas alcoholicas y tabaco', new_value='Bebidas alcohólicas y tabaco'),
	replace_value(col='indicador', curr_value='Recreacion y cultura', new_value='Recreación y cultura'),
	replace_value(col='indicador', curr_value='Educacion', new_value='Educación'),
	replace_value(col='indicador', curr_value='Vivienda agua electricidad gas y otros combustibles', new_value='Vivienda, agua, electricidad, gas y otros combustibles')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   region  84 non-null     object 
#   1   sector  84 non-null     object 
#   2   valor   84 non-null     float64
#  
#  |    | region   | sector                             |   valor |
#  |---:|:---------|:-----------------------------------|--------:|
#  |  0 | GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'geocodigo', 'sector': 'indicador'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | GBA         | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Nacional', new_value='AR-NAC')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | GBA         | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='GBA', new_value='AR-GBA')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='NOA', new_value='AR-NOA')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='NEA', new_value='AR-NEA')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Pampeana', new_value='AR-PAM')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Cuyo', new_value='AR-CUY')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Patagonia', new_value='AR-PAT')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcoholicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Alimentos y bebidas no alcoholicas', new_value='Alimentos y bebidas no alcohólicas')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcohólicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Bebidas alcoholicas y tabaco', new_value='Bebidas alcohólicas y tabaco')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcohólicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Recreacion y cultura', new_value='Recreación y cultura')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcohólicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Educacion', new_value='Educación')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcohólicas |    23.4 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Vivienda agua electricidad gas y otros combustibles', new_value='Vivienda, agua, electricidad, gas y otros combustibles')
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  84 non-null     object 
#   1   indicador  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    | geocodigo   | indicador                          |   valor |
#  |---:|:------------|:-----------------------------------|--------:|
#  |  0 | AR-GBA      | Alimentos y bebidas no alcohólicas |    23.4 |
#  
#  ------------------------------
#  