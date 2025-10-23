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
#  RangeIndex: 84400 entries, 0 to 84399
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             84400 non-null  int64  
#   1   geocodigoFundar  84400 non-null  object 
#   2   tipo_energia     84400 non-null  object 
#   3   valor_en_twh     60379 non-null  float64
#   4   porcentaje       84400 non-null  float64
#   5   geonombreFundar  84400 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |---:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  |  0 |   2000 | AFG               | Otras renovables |              0 |            0 | Afganistán        |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "WLD" & tipo_energia != "Total"')
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   tipo_energia     360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |          43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Bioenergia', new_value='Bioenergía')
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   tipo_energia     360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |          43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Carbon', new_value='Carbón')
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   tipo_energia     360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |          43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Petroleo', new_value='Petróleo')
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   tipo_energia     360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |          43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Eolica', new_value='Eólica')
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   tipo_energia     360 non-null    object 
#   3   valor_en_twh     360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | tipo_energia     |   valor_en_twh |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|---------------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |          43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   geocodigoFundar  360 non-null    object 
#   2   indicador        360 non-null    object 
#   3   valor            360 non-null    float64
#   4   porcentaje       360 non-null    float64
#   5   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | geocodigoFundar   | indicador        |   valor |   porcentaje | geonombreFundar   |
#  |-----:|-------:|:------------------|:-----------------|--------:|-------------:|:------------------|
#  | 8280 |   2000 | WLD               | Otras renovables |   43.65 |     0.285692 | Mundo             |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'porcentaje'], axis=1)
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   indicador        360 non-null    object 
#   2   valor            360 non-null    float64
#   3   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | indicador        |   valor | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------------|
#  | 8280 |   2000 | Otras renovables |   43.65 | Mundo             |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 360 entries, 8280 to 75839
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   indicador        360 non-null    object 
#   2   valor            360 non-null    float64
#   3   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | indicador        |   valor | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------------|
#  | 8280 |   2000 | Otras renovables |   43.65 | Mundo             |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Otras renovables': 1, 'Biocombustibles': 2, 'Solar': 3, 'Eólica': 4, 'Nuclear': 5, 'Hidro': 6, 'Gas natural': 7, 'Petróleo': 8, 'Carbón': 9}, prefix=['anio'], suffix=[])
#  Index: 360 entries, 8305 to 16744
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             360 non-null    int64  
#   1   indicador        360 non-null    object 
#   2   valor            360 non-null    float64
#   3   geonombreFundar  360 non-null    object 
#  
#  |      |   anio | indicador        |   valor | geonombreFundar   |
#  |-----:|-------:|:-----------------|--------:|:------------------|
#  | 8305 |   1985 | Otras renovables |       0 | Mundo             |
#  
#  ------------------------------
#  