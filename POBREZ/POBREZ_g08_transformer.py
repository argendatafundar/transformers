from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
def get_anios_mas_cercanos(df:DataFrame, group_col:str, anio_col:str = 'anio', anio:int = 2021, thresh:int = 3): 
    
    # lambda serie: serie.max() == maximo
    def es_anio_max(serie):
        maximo = serie.max()
        return serie == maximo

    df['bool_max'] = df.groupby(group_col)[anio_col].transform(es_anio_max)
    df['bool_select'] = abs(df[anio_col] - anio) <= thresh

    df = df.loc[df.bool_max & df.bool_select,:].drop(columns=['bool_max','bool_select'])
    return df

@transformer.convert
def concatenar_anio_a_pais(df:DataFrame, col_pais:str, col_anio): 
    df[col_pais] = df[col_pais].astype(str) + " (" + df[col_anio].astype(str) + ")"
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='poverty_line == 6.85'),
	drop_col(col='poverty_line', axis=1),
	rename_cols(map={'country_code': 'categoria', 'poverty_rate': 'valor'}),
	replace_value(col='categoria', curr_value='AGO', new_value='Angola'),
	replace_value(col='categoria', curr_value='ALB', new_value='Albania'),
	replace_value(col='categoria', curr_value='ARE', new_value='Emiratos Árabes Unidos'),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='ARM', new_value='Armenia'),
	replace_value(col='categoria', curr_value='AUS', new_value='Australia'),
	replace_value(col='categoria', curr_value='AUT', new_value='Austria'),
	replace_value(col='categoria', curr_value='BEL', new_value='Bélgica'),
	replace_value(col='categoria', curr_value='BEN', new_value='Benin'),
	replace_value(col='categoria', curr_value='BFA', new_value='Burkina Faso'),
	replace_value(col='categoria', curr_value='BGR', new_value='Bulgaria'),
	replace_value(col='categoria', curr_value='BLR', new_value='Belarús'),
	replace_value(col='categoria', curr_value='BOL', new_value='Bolivia'),
	replace_value(col='categoria', curr_value='BRA', new_value='Brasil'),
	replace_value(col='categoria', curr_value='BTN', new_value='Bhután'),
	replace_value(col='categoria', curr_value='CAN', new_value='Canadá'),
	replace_value(col='categoria', curr_value='CHE', new_value='Suiza'),
	replace_value(col='categoria', curr_value='CHL', new_value='Chile'),
	replace_value(col='categoria', curr_value='CHN', new_value='China'),
	replace_value(col='categoria', curr_value='CIV', new_value='Costa de Marfil'),
	replace_value(col='categoria', curr_value='COL', new_value='Colombia'),
	replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica'),
	replace_value(col='categoria', curr_value='CYP', new_value='Chipre'),
	replace_value(col='categoria', curr_value='CZE', new_value='Chequia'),
	replace_value(col='categoria', curr_value='DEU', new_value='Alemania'),
	replace_value(col='categoria', curr_value='DJI', new_value='Djibouti'),
	replace_value(col='categoria', curr_value='DNK', new_value='Dinamarca'),
	replace_value(col='categoria', curr_value='DOM', new_value='Rep. Dominicana'),
	replace_value(col='categoria', curr_value='ECU', new_value='Ecuador'),
	replace_value(col='categoria', curr_value='EGY', new_value='Egipto'),
	replace_value(col='categoria', curr_value='ESP', new_value='España'),
	replace_value(col='categoria', curr_value='EST', new_value='Estonia'),
	replace_value(col='categoria', curr_value='FIN', new_value='Finlandia'),
	replace_value(col='categoria', curr_value='FJI', new_value='Fiji'),
	replace_value(col='categoria', curr_value='FRA', new_value='Francia'),
	replace_value(col='categoria', curr_value='GAB', new_value='Gabón'),
	replace_value(col='categoria', curr_value='GBR', new_value='Reino Unido'),
	replace_value(col='categoria', curr_value='GEO', new_value='Georgia'),
	replace_value(col='categoria', curr_value='GIN', new_value='Guinea'),
	replace_value(col='categoria', curr_value='GNB', new_value='Guinea-Bissau'),
	replace_value(col='categoria', curr_value='GRC', new_value='Grecia'),
	replace_value(col='categoria', curr_value='HND', new_value='Honduras'),
	replace_value(col='categoria', curr_value='HRV', new_value='Croacia'),
	replace_value(col='categoria', curr_value='HUN', new_value='Hungría'),
	replace_value(col='categoria', curr_value='IDN', new_value='Indonesia'),
	replace_value(col='categoria', curr_value='IND', new_value='India'),
	replace_value(col='categoria', curr_value='IRL', new_value='Irlanda'),
	replace_value(col='categoria', curr_value='IRN', new_value='Irán'),
	replace_value(col='categoria', curr_value='ISL', new_value='Islandia'),
	replace_value(col='categoria', curr_value='ISR', new_value='Israel'),
	replace_value(col='categoria', curr_value='ITA', new_value='Italia'),
	replace_value(col='categoria', curr_value='KAZ', new_value='Kazajstán'),
	replace_value(col='categoria', curr_value='KGZ', new_value='Kirguistán'),
	replace_value(col='categoria', curr_value='KIR', new_value='Kiribati'),
	replace_value(col='categoria', curr_value='LAO', new_value='Laos'),
	replace_value(col='categoria', curr_value='LKA', new_value='Sri Lanka'),
	replace_value(col='categoria', curr_value='LSO', new_value='Lesotho'),
	replace_value(col='categoria', curr_value='LTU', new_value='Lituania'),
	replace_value(col='categoria', curr_value='LUX', new_value='Luxemburgo'),
	replace_value(col='categoria', curr_value='LVA', new_value='Letonia'),
	replace_value(col='categoria', curr_value='MDA', new_value='Moldavia'),
	replace_value(col='categoria', curr_value='MDV', new_value='Maldivas'),
	replace_value(col='categoria', curr_value='MEX', new_value='México'),
	replace_value(col='categoria', curr_value='MHL', new_value='Islas Marshall'),
	replace_value(col='categoria', curr_value='MKD', new_value='Macedonia del Norte'),
	replace_value(col='categoria', curr_value='MLI', new_value='Malí'),
	replace_value(col='categoria', curr_value='MLT', new_value='Malta'),
	replace_value(col='categoria', curr_value='MMR', new_value='Myanmar'),
	replace_value(col='categoria', curr_value='MNE', new_value='Montenegro'),
	replace_value(col='categoria', curr_value='MNG', new_value='Mongolia'),
	replace_value(col='categoria', curr_value='MOZ', new_value='Mozambique'),
	replace_value(col='categoria', curr_value='MUS', new_value='Mauricio'),
	replace_value(col='categoria', curr_value='MWI', new_value='Malawi'),
	replace_value(col='categoria', curr_value='MYS', new_value='Malasia'),
	replace_value(col='categoria', curr_value='NER', new_value='Níger'),
	replace_value(col='categoria', curr_value='NGA', new_value='Nigeria'),
	replace_value(col='categoria', curr_value='NLD', new_value='Países Bajos'),
	replace_value(col='categoria', curr_value='NOR', new_value='Noruega'),
	replace_value(col='categoria', curr_value='PAK', new_value='Pakistán'),
	replace_value(col='categoria', curr_value='PAN', new_value='Panamá'),
	replace_value(col='categoria', curr_value='PER', new_value='Perú'),
	replace_value(col='categoria', curr_value='PHL', new_value='Filipinas'),
	replace_value(col='categoria', curr_value='POL', new_value='Polonia'),
	replace_value(col='categoria', curr_value='PRT', new_value='Portugal'),
	replace_value(col='categoria', curr_value='PRY', new_value='Paraguay'),
	replace_value(col='categoria', curr_value='ROU', new_value='Rumania'),
	replace_value(col='categoria', curr_value='RUS', new_value='Rusia'),
	replace_value(col='categoria', curr_value='SEN', new_value='Senegal'),
	replace_value(col='categoria', curr_value='SLE', new_value='Sierra Leona'),
	replace_value(col='categoria', curr_value='SLV', new_value='El Salvador'),
	replace_value(col='categoria', curr_value='SRB', new_value='Serbia'),
	replace_value(col='categoria', curr_value='STP', new_value='Santo Tomé y Príncipe'),
	replace_value(col='categoria', curr_value='SVK', new_value='Eslovaquia'),
	replace_value(col='categoria', curr_value='SVN', new_value='Eslovenia'),
	replace_value(col='categoria', curr_value='SWE', new_value='Suecia'),
	replace_value(col='categoria', curr_value='SYC', new_value='Seychelles'),
	replace_value(col='categoria', curr_value='TCD', new_value='Chad'),
	replace_value(col='categoria', curr_value='TGO', new_value='Togo'),
	replace_value(col='categoria', curr_value='THA', new_value='Tailandia'),
	replace_value(col='categoria', curr_value='TUR', new_value='Turquía'),
	replace_value(col='categoria', curr_value='TZA', new_value='Tanzanía'),
	replace_value(col='categoria', curr_value='UGA', new_value='Uganda'),
	replace_value(col='categoria', curr_value='UKR', new_value='Ucrania'),
	replace_value(col='categoria', curr_value='URY', new_value='Uruguay'),
	replace_value(col='categoria', curr_value='USA', new_value='Estados Unidos'),
	replace_value(col='categoria', curr_value='VNM', new_value='Vietnam'),
	replace_value(col='categoria', curr_value='VUT', new_value='Vanuatu'),
	replace_value(col='categoria', curr_value='XKX', new_value='Kosovo'),
	replace_value(col='categoria', curr_value='ZWE', new_value='Zimbabwe'),
	get_anios_mas_cercanos(group_col='categoria', anio_col='year', anio=2019, thresh=3),
	concatenar_anio_a_pais(col_pais='categoria', col_anio='year'),
	mutiplicar_por_escalar(col='valor', k=100),
	drop_col(col='year', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 545 entries, 0 to 544
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  545 non-null    object 
#   1   year          545 non-null    int64  
#   2   poverty_line  545 non-null    float64
#   3   poverty_rate  545 non-null    float64
#  
#  |    | country_code   |   year |   poverty_line |   poverty_rate |
#  |---:|:---------------|-------:|---------------:|---------------:|
#  |  0 | AGO            |   2018 |           2.15 |        0.31122 |
#  
#  ------------------------------
#  
#  query(condition='poverty_line == 6.85')
#  Index: 109 entries, 218 to 326
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  109 non-null    object 
#   1   year          109 non-null    int64  
#   2   poverty_line  109 non-null    float64
#   3   poverty_rate  109 non-null    float64
#  
#  |     | country_code   |   year |   poverty_line |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|---------------:|
#  | 218 | AGO            |   2018 |           6.85 |       0.779747 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   country_code  109 non-null    object 
#   1   year          109 non-null    int64  
#   2   poverty_rate  109 non-null    float64
#  
#  |     | country_code   |   year |   poverty_rate |
#  |----:|:---------------|-------:|---------------:|
#  | 218 | AGO            |   2018 |       0.779747 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'categoria', 'poverty_rate': 'valor'})
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | AGO         |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='AGO', new_value='Angola')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ALB', new_value='Albania')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARE', new_value='Emiratos Árabes Unidos')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARM', new_value='Armenia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='AUS', new_value='Australia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='AUT', new_value='Austria')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BEL', new_value='Bélgica')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BEN', new_value='Benin')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BFA', new_value='Burkina Faso')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BGR', new_value='Bulgaria')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BLR', new_value='Belarús')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BOL', new_value='Bolivia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BRA', new_value='Brasil')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='BTN', new_value='Bhután')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CAN', new_value='Canadá')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHE', new_value='Suiza')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHL', new_value='Chile')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CHN', new_value='China')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CIV', new_value='Costa de Marfil')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='COL', new_value='Colombia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CRI', new_value='Costa Rica')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CYP', new_value='Chipre')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='CZE', new_value='Chequia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DEU', new_value='Alemania')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DJI', new_value='Djibouti')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DNK', new_value='Dinamarca')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='DOM', new_value='Rep. Dominicana')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ECU', new_value='Ecuador')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='EGY', new_value='Egipto')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ESP', new_value='España')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='EST', new_value='Estonia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='FIN', new_value='Finlandia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='FJI', new_value='Fiji')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='FRA', new_value='Francia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GAB', new_value='Gabón')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GBR', new_value='Reino Unido')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GEO', new_value='Georgia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GIN', new_value='Guinea')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GNB', new_value='Guinea-Bissau')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='GRC', new_value='Grecia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='HND', new_value='Honduras')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='HRV', new_value='Croacia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='HUN', new_value='Hungría')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='IDN', new_value='Indonesia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='IND', new_value='India')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='IRL', new_value='Irlanda')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='IRN', new_value='Irán')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ISL', new_value='Islandia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ISR', new_value='Israel')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ITA', new_value='Italia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='KAZ', new_value='Kazajstán')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='KGZ', new_value='Kirguistán')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='KIR', new_value='Kiribati')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LAO', new_value='Laos')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LKA', new_value='Sri Lanka')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LSO', new_value='Lesotho')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LTU', new_value='Lituania')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LUX', new_value='Luxemburgo')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LVA', new_value='Letonia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MDA', new_value='Moldavia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MDV', new_value='Maldivas')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MEX', new_value='México')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MHL', new_value='Islas Marshall')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MKD', new_value='Macedonia del Norte')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MLI', new_value='Malí')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MLT', new_value='Malta')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MMR', new_value='Myanmar')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MNE', new_value='Montenegro')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MNG', new_value='Mongolia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MOZ', new_value='Mozambique')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MUS', new_value='Mauricio')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MWI', new_value='Malawi')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='MYS', new_value='Malasia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='NER', new_value='Níger')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='NGA', new_value='Nigeria')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='NLD', new_value='Países Bajos')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='NOR', new_value='Noruega')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAK', new_value='Pakistán')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PAN', new_value='Panamá')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PER', new_value='Perú')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PHL', new_value='Filipinas')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='POL', new_value='Polonia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRT', new_value='Portugal')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='PRY', new_value='Paraguay')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ROU', new_value='Rumania')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='RUS', new_value='Rusia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SEN', new_value='Senegal')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLE', new_value='Sierra Leona')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SLV', new_value='El Salvador')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SRB', new_value='Serbia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='STP', new_value='Santo Tomé y Príncipe')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SVK', new_value='Eslovaquia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SVN', new_value='Eslovenia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SWE', new_value='Suecia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='SYC', new_value='Seychelles')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='TCD', new_value='Chad')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='TGO', new_value='Togo')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='THA', new_value='Tailandia')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='TUR', new_value='Turquía')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='TZA', new_value='Tanzanía')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='UGA', new_value='Uganda')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='UKR', new_value='Ucrania')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='URY', new_value='Uruguay')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='USA', new_value='Estados Unidos')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='VNM', new_value='Vietnam')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='VUT', new_value='Vanuatu')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='XKX', new_value='Kosovo')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria   |   year |    valor |
#  |----:|:------------|-------:|---------:|
#  | 218 | Angola      |   2018 | 0.779747 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ZWE', new_value='Zimbabwe')
#  Index: 109 entries, 218 to 326
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   categoria    109 non-null    object 
#   1   year         109 non-null    int64  
#   2   valor        109 non-null    float64
#   3   bool_max     109 non-null    bool   
#   4   bool_select  109 non-null    bool   
#  
#  |     | categoria   |   year |    valor | bool_max   | bool_select   |
#  |----:|:------------|-------:|---------:|:-----------|:--------------|
#  | 218 | Angola      |   2018 | 0.779747 | True       | True          |
#  
#  ------------------------------
#  
#  get_anios_mas_cercanos(group_col='categoria', anio_col='year', anio=2019, thresh=3)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria     |   year |   valor |
#  |----:|:--------------|-------:|--------:|
#  | 218 | Angola (2018) |   2018 | 77.9747 |
#  
#  ------------------------------
#  
#  concatenar_anio_a_pais(col_pais='categoria', col_anio='year')
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria     |   year |   valor |
#  |----:|:--------------|-------:|--------:|
#  | 218 | Angola (2018) |   2018 | 77.9747 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 109 entries, 218 to 326
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   year       109 non-null    int64  
#   2   valor      109 non-null    float64
#  
#  |     | categoria     |   year |   valor |
#  |----:|:--------------|-------:|--------:|
#  | 218 | Angola (2018) |   2018 | 77.9747 |
#  
#  ------------------------------
#  
#  drop_col(col='year', axis=1)
#  Index: 109 entries, 218 to 326
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  109 non-null    object 
#   1   valor      109 non-null    float64
#  
#  |     | categoria     |   valor |
#  |----:|:--------------|--------:|
#  | 218 | Angola (2018) | 77.9747 |
#  
#  ------------------------------
#  