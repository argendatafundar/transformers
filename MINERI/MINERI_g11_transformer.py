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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'vab_min_provincial': 'valor', 'provincia': 'geoselector'}),
	replace_value(col='geoselector', curr_value='Buenos_Aires', new_value='AR-B'),
	replace_value(col='geoselector', curr_value='Catamarca', new_value='AR-K'),
	replace_value(col='geoselector', curr_value='Chaco', new_value='AR-H'),
	replace_value(col='geoselector', curr_value='Chubut', new_value='AR-U'),
	replace_value(col='geoselector', curr_value='Ciudad_de_Buenos_Aires', new_value='AR-C'),
	replace_value(col='geoselector', curr_value='Cordoba', new_value='AR-X'),
	replace_value(col='geoselector', curr_value='Corrientes', new_value='AR-W'),
	replace_value(col='geoselector', curr_value='Entre_Rios', new_value='AR-E'),
	replace_value(col='geoselector', curr_value='Formosa', new_value='AR-P'),
	replace_value(col='geoselector', curr_value='Jujuy', new_value='AR-Y'),
	replace_value(col='geoselector', curr_value='La_Pampa', new_value='AR-L'),
	replace_value(col='geoselector', curr_value='La_Rioja', new_value='AR-F'),
	replace_value(col='geoselector', curr_value='Mendoza', new_value='AR-M'),
	replace_value(col='geoselector', curr_value='Misiones', new_value='AR-N'),
	replace_value(col='geoselector', curr_value='Neuquen', new_value='AR-Q'),
	replace_value(col='geoselector', curr_value='Rio_Negro', new_value='AR-R'),
	replace_value(col='geoselector', curr_value='Salta', new_value='AR-A'),
	replace_value(col='geoselector', curr_value='San_Juan', new_value='AR-J'),
	replace_value(col='geoselector', curr_value='San_Luis', new_value='AR-D'),
	replace_value(col='geoselector', curr_value='Santa_Cruz', new_value='AR-Z'),
	replace_value(col='geoselector', curr_value='Santa_Fe', new_value='AR-S'),
	replace_value(col='geoselector', curr_value='Santiago_del_Estero', new_value='AR-G'),
	replace_value(col='geoselector', curr_value='Tierra_del_Fuego', new_value='AR-V'),
	replace_value(col='geoselector', curr_value='Tucuman', new_value='AR-T'),
	replace_value(col='geoselector', curr_value='No_distribuido', new_value='NO-DIST-MINERI')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   provincia           475 non-null    object 
#   1   anio                475 non-null    int64  
#   2   vab_min_provincial  475 non-null    float64
#  
#  |    | provincia              |   anio |   vab_min_provincial |
#  |---:|:-----------------------|-------:|---------------------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 |              112.651 |
#  
#  ------------------------------
#  
#  rename_cols(map={'vab_min_provincial': 'valor', 'provincia': 'geoselector'})
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector            |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Buenos_Aires', new_value='AR-B')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector            |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Catamarca', new_value='AR-K')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector            |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Chaco', new_value='AR-H')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector            |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Chubut', new_value='AR-U')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector            |   anio |   valor |
#  |---:|:-----------------------|-------:|--------:|
#  |  0 | Ciudad_de_Buenos_Aires |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Ciudad_de_Buenos_Aires', new_value='AR-C')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Cordoba', new_value='AR-X')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Corrientes', new_value='AR-W')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Entre_Rios', new_value='AR-E')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Formosa', new_value='AR-P')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Jujuy', new_value='AR-Y')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='La_Pampa', new_value='AR-L')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='La_Rioja', new_value='AR-F')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Mendoza', new_value='AR-M')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Misiones', new_value='AR-N')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Neuquen', new_value='AR-Q')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Rio_Negro', new_value='AR-R')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Salta', new_value='AR-A')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='San_Juan', new_value='AR-J')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='San_Luis', new_value='AR-D')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Santa_Cruz', new_value='AR-Z')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Santa_Fe', new_value='AR-S')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Santiago_del_Estero', new_value='AR-G')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Tierra_del_Fuego', new_value='AR-V')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='Tucuman', new_value='AR-T')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='No_distribuido', new_value='NO-DIST-MINERI')
#  RangeIndex: 475 entries, 0 to 474
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  475 non-null    object 
#   1   anio         475 non-null    int64  
#   2   valor        475 non-null    float64
#  
#  |    | geoselector   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | AR-C          |   2004 | 112.651 |
#  
#  ------------------------------
#  