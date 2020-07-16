import argparse
import logging
logging.basicConfig(level=logging.INFO)
import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def main(filename_db,filename_t,category,today):

    logger.info('Starting loading process')

    df_db = _read_data(filename_db)
    df_t = _read_data(filename_t)

    df_all = _merge_df(df_db,df_t)
    df = _concate_rows(df_db,df_t,df_all)
    df = _adding_last_price_column(df, df_all, today)

    marks=_marks_by_category(category)
    df = _find_marks(df,marks)

    df.to_csv(category+'_db.csv')
    return df

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))
    df = pd.read_csv(filename)
    df = df.set_index('uid')

    return df

def _merge_df(df_db,df_t):
    logger.info('Starting merge')
    logger.info('DB articles {}'.format(df_db.shape[0]))
    logger.info('Today articles {}'.format(df_t.shape[0]))
    df=pd.merge(df_db, df_t,
                left_on='uid', right_on='uid', 
                suffixes=('_db', '_t'), 
                indicator=True,how='outer')
    logger.info('Merge Total articles {}'.format(df.shape[0]))

    return df

def _concate_rows(df_db,db_t,df_all):
    logger.info('Concate new products to DB')
    df_new = df_all[df_all['_merge']=='right_only']
    logger.info('Finding {} new products'.format(df_new.shape[0]))
    logger.info('Selecting DB columns for new objects')
    df_new = df_new.loc[:,['categoria_t','producto_t','link_t','imagen_t']]
    logger.info('Renaming columns for new objects')
    df_new.rename(columns={'categoria_t':'categoria',
                          'producto_t':'producto',
                          'link_t':'link',
                          'imagen_t':'imagen'}, 
                 inplace=True)
    logger.info('New objects to add: {}'.format(df_new.shape[0]))
    df = pd.concat([df_db, df_new], verify_integrity=True)
    df.fillna(-1, inplace=True)
    logger.info('Drop duplicated links articles')
    
    return df.drop_duplicates(subset=['link'],keep='first')

def _adding_last_price_column(df, df_all, today):
    df[today]=np.where(df_all['_merge']!='left_only', 
                            df_all[today], -1)
    return df

def _marks_by_category(category):    
    switcher = {
        'telefonos':             ['samsung','iphone','huawei','xiaomi','motorola','nokia','lg ','alcatel','kalley','asus'],
        'computadores-tablets':  ['acer','hp','lenovo','asus','mac','ipad','lg ','huawei','epson','benq','samsung','rog','aoc'],
        'televisores':           ['lg ','samsung','sony','panasonic','challenger','hyundai','aoc','kalley','philips'],
        'electrodomesticos':     ['abba','oster','kalley','challenger','haceb','mabe','whirlpool','electrolux','black & decker','samsung','lg','samurai','remington','universal','imusa','superior','gama','kitchenaid','boccherini','hamilton','general electric','karcher','panasonic','philips','westinghouse','babyliss','conair','multitech','cuisinart','singer','wahl','centrales','honeywell','ninja','sunbeam','t-fal','bionaire'],
        'audio':                 ['bose','jbl','sony','kalley','samsung','lg ','yamaha','esenses','panasonic','better','multitech','hp','pioneer','philips','klipxtreme','apple','earpods','huawei','klipsch','xiaomi','hyundai','braven','xtech','google','ifrogz','logitech','thrustmaster','hyperx','xcb'],
        'video-juegos':          ['ps4','xbox','funko','nintendo'],
        'accesorios':            ['kalley','hp','apple','ipad','iphone','belkin','bestcom','samsung','adata','techtex','bose','case logic','microsoft','targus','lenovo','technosoportes','huawei','marcar','thule','techbag','magom','kingston','x-kim','forza','klipxtreme','multitech','startec','sandisk','logitech','tp-link','esenses','sony','funko','verbatim','kanex','jbl','fitbit','google','xcb','gopro','xiaomi','altigo','legion','primus','motorola','toshiba','wacom','zagg','thrustmaster','linksys','ifrogz','kenex','hyoerx','emmtec','e4u','lg ','acer','nintendo','nexxt','mcafee','lite on','tapo'],
        'camaras':               ['canon','sony','gopro','olympus'],
        'netflix-otros':         ['xbox','virgin','spotify','microsoft','playstation','netflix','imvu','kaspersky'],
        'smartwatch':            ['samsung','fitbit','apple','huawei','polar','xiaomi'],
        'deportes':              ['evo','aktive','emove','polar','sportop','proform','healthy sports','powerfit','vitanas','nordictrack','atacama','bisto','body shaper','cybex','fitbit'],
        'hogar-muebles':         ['tukasa','practimac','inval','maderkit','dko design','vanyplas','rimax'],
    }

    return switcher.get(category,"nothing")

def _find_marks(df,marks):
    logger.info('Starting mark finding')
    df['marca']='otros'
    for mark in marks:
        df['marca']=(df
                    .apply(lambda row: row['producto'],axis=1)
                    .apply(lambda product: mark.capitalize() if (product.lower().find(mark)!=-1) else df['marca'][df.index[df['producto']==product].tolist()].tolist()[0])
        )

    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename_db',
                        help='The path to the DB csv',
                        type=str)

    parser.add_argument('filename_t',
                        help='The path to the clean data new',
                        type=str)

    parser.add_argument('category',
                        help='Category to merge',
                        type=str)
    
    parser.add_argument('today',
                        help='Date of new data to add',
                        type=str)

    args = parser.parse_args()

    df = main(args.filename_db,args.filename_t,args.category,args.today)