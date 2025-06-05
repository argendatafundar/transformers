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
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def to_pandas(df: pl.DataFrame):
    import pandas as pd
    return df.to_pandas()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(),
	query(condition='iso3 == "ARG"'),
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
#  
#  ------------------------------
#  
#  to_pandas()
#  RangeIndex: 38938 entries, 0 to 38937
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            38938 non-null  int64  
#   1   iso3            38938 non-null  object 
#   2   fuente_energia  38938 non-null  object 
#   3   valor_en_twh    38938 non-null  float64
#   4   porcentaje      38938 non-null  float64
#   5   tipo_energia    38938 non-null  object 
#  
#  |    |   anio | iso3   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|:---------------|
#  |  0 |   1971 | DZA    | Otras renovables |              0 |            0 | Limpias        |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            531 non-null    int64  
#   1   iso3            531 non-null    object 
#   2   fuente_energia  531 non-null    object 
#   3   valor_en_twh    531 non-null    float64
#   4   porcentaje      531 non-null    float64
#   5   tipo_energia    531 non-null    object 
#  
#  |    |   anio | iso3   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   |
#  |---:|-------:|:-------|:-----------------|---------------:|-------------:|:---------------|
#  | 59 |   1971 | ARG    | Otras renovables |       0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   porcentaje    531 non-null    float64
#   5   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor |   porcentaje | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|-------------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  query(condition='indicador != "Total"')
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   porcentaje    531 non-null    float64
#   5   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor |   porcentaje | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|-------------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Carbon', new_value='Carbón')
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   porcentaje    531 non-null    float64
#   5   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor |   porcentaje | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|-------------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo')
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   porcentaje    531 non-null    float64
#   5   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor |   porcentaje | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|-------------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Eolica', new_value='Eólica')
#  Index: 531 entries, 59 to 34596
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   porcentaje    531 non-null    float64
#   5   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor |   porcentaje | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|-------------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |    0.0502508 | Limpias        |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  Index: 531 entries, 59 to 34596
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          531 non-null    int64  
#   1   geocodigo     531 non-null    object 
#   2   indicador     531 non-null    object 
#   3   valor         531 non-null    float64
#   4   tipo_energia  531 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |    valor | tipo_energia   |
#  |---:|-------:|:------------|:-----------------|---------:|:---------------|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 | Limpias        |
#  
#  ------------------------------
#  
#  drop_col(col='tipo_energia', axis=1)
#  Index: 531 entries, 59 to 34596
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       531 non-null    int64  
#   1   geocodigo  531 non-null    object 
#   2   indicador  531 non-null    object 
#   3   valor      531 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador        |    valor |
#  |---:|-------:|:------------|:-----------------|---------:|
#  | 59 |   1971 | ARG         | Otras renovables | 0.188311 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['geocodigo', 'anio'], suffix=[])
#  Index: 531 entries, 112 to 30131
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       531 non-null    int64  
#   1   geocodigo  531 non-null    object 
#   2   indicador  531 non-null    object 
#   3   valor      531 non-null    float64
#  
#  |     |   anio | geocodigo   | indicador        |   valor |
#  |----:|-------:|:------------|:-----------------|--------:|
#  | 112 |   1965 | ARG         | Otras renovables |       0 |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  Index: 531 entries, 112 to 30131
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       531 non-null    int64  
#   1   geocodigo  531 non-null    object 
#   2   indicador  531 non-null    object 
#   3   valor      531 non-null    float64
#  
#  |     |   anio | geocodigo   | indicador        |   valor |
#  |----:|-------:|:------------|:-----------------|--------:|
#  | 112 |   1965 | ARG         | Otras renovables |       0 |
#  
#  ------------------------------
#  