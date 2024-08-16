from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'}),
	query(condition='indicador != "Total"'),
	replace_value(col='indicador', curr_value='Carbon', new_value='Carbón'),
	replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo'),
	replace_value(col='indicador', curr_value='Eolica', new_value='Eólica'),
	drop_col(col='porcentaje', axis=1),
	drop_col(col='tipo_energia', axis=1),
	sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['geocodigo', 'anio'], suffix=[]),
	drop_na(subset=['valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 52923 entries, 0 to 52922
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            52923 non-null  object 
#   1   anio            52923 non-null  int64  
#   2   fuente_energia  52923 non-null  object 
#   3   tipo_energia    52923 non-null  object 
#   4   valor_en_twh    41782 non-null  float64
#   5   porcentaje      52923 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 52923 entries, 0 to 52922
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            52923 non-null  object 
#   1   anio            52923 non-null  int64  
#   2   fuente_energia  52923 non-null  object 
#   3   tipo_energia    52923 non-null  object 
#   4   valor_en_twh    41782 non-null  float64
#   5   porcentaje      52923 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 52923 entries, 0 to 52922
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     52923 non-null  object 
#   1   anio          52923 non-null  int64  
#   2   indicador     52923 non-null  object 
#   3   tipo_energia  52923 non-null  object 
#   4   valor         41782 non-null  float64
#   5   porcentaje    52923 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  query(condition='indicador != "Total"')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor         35292 non-null  float64
#   5   porcentaje    46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Carbon', new_value='Carbón')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor         35292 non-null  float64
#   5   porcentaje    46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor         35292 non-null  float64
#   5   porcentaje    46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Eolica', new_value='Eólica')
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor         35292 non-null  float64
#   5   porcentaje    46433 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     46433 non-null  object 
#   1   anio          46433 non-null  int64  
#   2   indicador     46433 non-null  object 
#   3   tipo_energia  46433 non-null  object 
#   4   valor         35292 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |
#  
#  ------------------------------
#  
#  drop_col(col='tipo_energia', axis=1)
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  46433 non-null  object 
#   1   anio       46433 non-null  int64  
#   2   indicador  46433 non-null  object 
#   3   valor      35292 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |     nan |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['geocodigo', 'anio'], suffix=[])
#  Index: 46433 entries, 0 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  46433 non-null  object 
#   1   anio       46433 non-null  int64  
#   2   indicador  46433 non-null  object 
#   3   valor      35292 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |     nan |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  Index: 35292 entries, 59 to 52863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  35292 non-null  object 
#   1   anio       35292 non-null  int64  
#   2   indicador  35292 non-null  object 
#   3   valor      35292 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador   |   valor |
#  |---:|:------------|-------:|:------------|--------:|
#  | 59 | AGO         |   1965 | Nuclear     |       0 |
#  
#  ------------------------------
#  