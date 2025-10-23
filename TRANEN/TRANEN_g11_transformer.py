from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    return df.dropna(subset=cols)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "WLD" & tipo_energia != "Total"'),
	replace_value(col='tipo_energia', curr_value='Bioenergia', new_value='Bioenergía'),
	replace_value(col='tipo_energia', curr_value='Carbon', new_value='Carbón'),
	replace_value(col='tipo_energia', curr_value='Petroleo', new_value='Petróleo'),
	replace_value(col='tipo_energia', curr_value='Eolica', new_value='Eólica'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	drop_col(col=['geocodigoFundar', 'porcentaje'], axis=1),
	drop_na(cols=['valor']),
	sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Otras renovables': 1, 'Biocombustibles': 2, 'Solar': 3, 'Eólica': 4, 'Nuclear': 5, 'Hidro': 6, 'Gas natural': 7, 'Petróleo': 8, 'Carbón': 9}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 40851 entries, 0 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             40851 non-null  int64  
#   1   geocodigoFundar  40851 non-null  object 
#   2   fuente_energia   40851 non-null  object 
#   3   valor_en_twh     40851 non-null  float64
#   4   porcentaje       40851 non-null  float64
#   5   tipo_energia     40851 non-null  object 
#   6   geonombreFundar  40851 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  |  0 |   1965 | DZA               | Otras renovables |              0 |            0 | Limpias        | Argelia           |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "WLD" & tipo_energia != "Total"')
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables |        56.2039 |     0.130121 | Limpias        | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Bioenergia', new_value='Bioenergía')
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables |        56.2039 |     0.130121 | Limpias        | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Carbon', new_value='Carbón')
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables |        56.2039 |     0.130121 | Limpias        | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Petroleo', new_value='Petróleo')
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables |        56.2039 |     0.130121 | Limpias        | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Eolica', new_value='Eólica')
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor_en_twh     540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables |        56.2039 |     0.130121 | Limpias        | Mundo             |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigoFundar  540 non-null    object 
#   2   fuente_energia   540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   indicador        540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | fuente_energia   |   valor |   porcentaje | indicador   | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|--------:|-------------:|:------------|:------------------|
#  | 4479 |   1965 | WLD               | Otras renovables | 56.2039 |     0.130121 | Limpias     | Mundo             |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'porcentaje'], axis=1)
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   fuente_energia   540 non-null    object 
#   2   valor            540 non-null    float64
#   3   indicador        540 non-null    object 
#   4   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | fuente_energia   |   valor | indicador   | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------|:------------------|
#  | 4479 |   1965 | Otras renovables | 56.2039 | Limpias     | Mundo             |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   fuente_energia   540 non-null    object 
#   2   valor            540 non-null    float64
#   3   indicador        540 non-null    object 
#   4   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | fuente_energia   |   valor | indicador   | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------|:------------------|
#  | 4479 |   1965 | Otras renovables | 56.2039 | Limpias     | Mundo             |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Otras renovables': 1, 'Biocombustibles': 2, 'Solar': 3, 'Eólica': 4, 'Nuclear': 5, 'Hidro': 6, 'Gas natural': 7, 'Petróleo': 8, 'Carbón': 9}, prefix=['anio'], suffix=[])
#  Index: 540 entries, 4479 to 40850
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   fuente_energia   540 non-null    object 
#   2   valor            540 non-null    float64
#   3   indicador        540 non-null    object 
#   4   geonombreFundar  540 non-null    object 
#  
#  |      |   anio | fuente_energia   |   valor | indicador   | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------|:------------------|
#  | 4479 |   1965 | Otras renovables | 56.2039 | Limpias     | Mundo             |
#  
#  ------------------------------
#  