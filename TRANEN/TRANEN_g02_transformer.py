from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'geocodigoFundar': 'geocodigo'}),
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
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 540 entries, 60 to 36431
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
#  |    |   anio | geocodigoFundar   | fuente_energia   |   valor_en_twh |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG               | Otras renovables |              0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'geocodigoFundar': 'geocodigo'})
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  query(condition='indicador != "Total"')
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Carbon', new_value='Carbón')
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo')
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Eolica', new_value='Eólica')
#  Index: 540 entries, 60 to 36431
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   porcentaje       540 non-null    float64
#   5   tipo_energia     540 non-null    object 
#   6   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor |   porcentaje | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|-------------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 |            0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  Index: 540 entries, 60 to 36431
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   tipo_energia     540 non-null    object 
#   5   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor | tipo_energia   | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|:---------------|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 | Limpias        | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='tipo_energia', axis=1)
#  Index: 540 entries, 60 to 36431
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 | Argentina         |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['geocodigo', 'anio'], suffix=[])
#  Index: 540 entries, 60 to 31892
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 | Argentina         |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  Index: 540 entries, 60 to 31892
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             540 non-null    int64  
#   1   geocodigo        540 non-null    object 
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#   4   geonombreFundar  540 non-null    object 
#  
#  |    |   anio | geocodigo   | indicador        |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:-----------------|--------:|:------------------|
#  | 60 |   1965 | ARG         | Otras renovables |       0 | Argentina         |
#  
#  ------------------------------
#  