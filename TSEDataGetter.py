
import pandas as pd
import requests
import numpy as np
import datetime
import time
import re
# import jdatetime

def intraday_trade_details(symbol_id , date):

    url = f'http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{symbol_id}/{date}/true'
    import requests
    User_Agent = "Mozilla/5.0 (X11; Ubunt`u; Linux x86_64; rv:105.0) Gecko/20100101 Firefox/105.0"
    host =  "cdn.tsetmc.com"
    accept =  "application/json, text/plain, */*"
    accept_language = "en-US,en;q=0.5"
    accept_encoding = "gzip, deflate"
    connection =  "keep-alive"
    headers = {"user-agent": User_Agent, "Host": host, "Accept": accept,
                "Accept-Language": accept_language, "Accept-Encoding": accept_encoding,
                "Connection":connection}
    r = requests.get(url , timeout=4 , headers=headers)
    df = pd.DataFrame(r.json()['tradeHistory'])
    df = df[df['canceled'] != 1]
    df['time'] = df['hEven'].apply( lambda x: datetime.datetime.strptime(str(x) , '%H%M%S').time())
    df.rename(columns={'pTran' :'price'  , 'qTitTran':'volume'} , inplace=True)
    df.sort_values('time', inplace=True)
    
    df_spare= pd.DataFrame(columns= ['volume','price','time'])
    df_spare['time']=pd.date_range(start="20230913",end="20230914",freq='1S').time
    df = df_spare.merge(df, on='time',suffixes=['_x' , '_y'],how='outer')
    df = df[(df['time'] > datetime.time(9,0))  & (df['time'] < datetime.time(12,30))]
    df.rename(columns={'price_y' :'price'  , 'volume_y':'volume'} , inplace=True)

    return df[['time','price', 'volume']]


def get_daily_trades_statistics(_id, dates):
    User_Agent = "Mozilla/5.0 (X11; Ubunt`u; Linux x86_64; rv:105.0) Gecko/20100101 Firefox/105.0"
    host =  "cdn.tsetmc.com"
    accept =  "application/json, text/plain, */*"
    accept_language = "en-US,en;q=0.5"
    accept_encoding = "gzip, deflate"
    connection =  "keep-alive"
    headers = {"user-agent": User_Agent, "Host": host, "Accept": accept,
               "Accept-Language": accept_language, "Accept-Encoding": accept_encoding,
               "Connection":connection}
    daily_trades = pd.DataFrame()
    for date in dates:
        print(date)
        url = f'http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDaily/{_id}/{date}'
        r = requests.get(url, headers=headers)
        df = pd.DataFrame(r.json()['closingPriceDaily'], index=[0])
        df = df[['insCode', 'dEven', 'pClosing', 'pDrCotVal', 'qTotTran5J', 'qTotCap']]
        df.columns = ['id', 'date', 'close_price', 'last_price', 'volume', 'value']
        daily_trades = pd.concat([daily_trades, df])
        time.sleep(1)
    daily_trades = daily_trades.drop_duplicates().reset_index(drop=True)
    print('----------------------------------------')
    return daily_trades



def trade_history_symbol(symbol_id):
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    url = f'http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{symbol_id}/0'
    data = requests.get(url,timeout=5,verify=False, headers=headers).json()
    columns = ['change_last_price', 'min_price' , 'max_price',
               'yesterday_price' , 'first_price', 'islast' , 'id_' , 'symbol_id',
               'date' , 'time' , 'close_price' , 'iClose_' , 'yClose_','last_price',
               'number_of_trades' , 'volume' , 'value']
    df = pd.DataFrame(data['closingPriceDaily'])
    df.columns = columns
    
    df['date']= pd.to_datetime(df.date,format='%Y%m%d')
    df['time']= pd.to_datetime(df.time,format='%H%M%S').dt.time
    
    # df.set_index('date',inplace=True)

    return df

def calculate_covered_call_interest_rate(option, op_col_name):
    interest_rate = np.nan
    if option[op_col_name]!=0:
        execution_com = 0.0055

        if option['symbol_base'] == 'اهرم':
            buy_base_com = 0.00116
        else:
            buy_base_com = 0.003632


        k = option['due_date_price']*(1-execution_com)
        
        if option['ask_price_1_base']!=0:
            p = option['ask_price_1_base']*(1+buy_base_com)
        elif option['bid_price_1_base']==option['max_allowed_price_base']:
            p = option['max_allowed_price_base']*(1+buy_base_com)
        else:
            return np.nan
        
        c = option[op_col_name]
        t = option['days_until_due']+2
        if c<p:
            interest_rate = (k/(p-c))**(round(365/t, 4))-1
    return interest_rate



def market_mapper(code):
    market_mappperdict = {'311' : 'calloption',
                    '309' : 'yellowIFB',
                    '303' : 'secondryIFB',
                    '305' : 'funds' ,
                    '300' : 'Bourse_symbols', '306':'Finance&bounds',
                    '320' : 'IFB_calloptions', '208':'sokook',
                    '312' : 'putoption' , '706':'MorabeheDolati',
                    '301' : 'CityMosharekat' , '701':'Saffron','307':'RealState',
                    '327' : 'CementKala' , '321' :'IFB_putoptions', '380' : 'Funds_Kala',
                    '404' : 'IFB_paaye_Advanceright_hagh','304':'future', 
                    '206' : 'GAMbounds','400':'Advanceright_hagh', '403':'IFB_Advanceright_hagh',
                    '313' : 'bourseKala_other', '308' : 'boursekala_self', '600': 'Tabaee_put'
                    
                    }
    return market_mappperdict.get(code)
def readable(n):
    abs_n = abs(n)
    if abs_n >= 1e7 and abs_n<= 1e10 :
        round_number = n/1e7
        human_readable = '{:,.0f}{}'.format(round_number,   ' میلیون تومان ')
    elif abs_n>1e10 and abs_n<= 1e13 :
        round_number = n/1e10
        
        human_readable = '{:,.2f}{}'.format(round_number, ' میلیارد تومان ')
    elif abs_n > 1e13:
        round_number = n/1e13
        human_readable = '{:,.2f}{}'.format(round_number, ' همت')
    else:
        round_number = n/10
        human_readable = '{:,.1f}{}'.format(round_number, 'تومان')
        
    return (human_readable)
def market_columns(number_of_columns):
    if number_of_columns == 25:
        return ['id' , 'isin', 'symbol','name','time',
            'first_price','close_price' , 'last_trade','number_trades',
            'volume', 'value','low_price' ,'high_price',
            'yesterday_price', 'eps' , 'base_volume', 'table_id',
            'industry_id' ,'section_code', 'max_allowed_price','min_allowed_price',
            'number_shares', 'type_of_asset','NAV','openInterest']
    elif number_of_columns == 23:
        return ['id' , 'isin', 'symbol','name','time',
            'first_price','close_price' , 'last_trade','number_trades',
            'volume', 'value','low_price' ,'high_price',
            'yesterday_price', 'eps' , 'base_volume', 'table_id',
            'industry_id' ,'section_code', 'max_allowed_price','min_allowed_price',
            'number_shares', 'type_of_asset']

def market_fetcher():
    parts = None
    try:
        url_list = ['http://old.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0',
                    'http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx?h=0&r=0']
        for _ in range(5):
            import random
            url = random.choice(url_list)
            data = requests.get(url_list[1], timeout=12)
            content = data.content.decode('utf-8')
            parts = content.split('@')
            if data.status_code==200:
                break
        
    except:
        time.sleep(1)
        pass
    return parts


def base_market_dataframe(parts):
    market = parts[2].split(';')
    while True:
        try:
            number_of_columns = len(market[10].split(','))
            df = pd.DataFrame([x.split(',') for x in market], columns=market_columns(number_of_columns=number_of_columns))
            df['type_of_asset'] = df['type_of_asset'].apply(market_mapper)
            df = df.apply(pd.to_numeric, errors='ignore')
            df['symbol'] = df['symbol'].apply(convert_ar_characters)
            df['name'] = df['name'].apply(convert_ar_characters) 
            break
        except:
            time.sleep(1)
    
    return df

def orderbook_dataframe(parts):
    orderbook = parts[3].split(';')
    columns_orderbook = ['id' , 'location' , 'n_orderSell','n_orderBuy',
                        'bid_price' , 'ask_price' , 'bid_vol','ask_vol']
    df2 = pd.DataFrame([x.split(',') for x in orderbook],columns=columns_orderbook)
    df2_pivot = df2.pivot(index='id' , columns='location')
    df2_pivot.columns = ["_".join((i,j)) for i,j in df2_pivot.columns]

    df2_pivot.reset_index(inplace=True)
    df2_pivot = df2_pivot.apply(pd.to_numeric, errors='ignore')
    return df2_pivot


def convert_ar_characters(input_str):
    mapping = {
        'ك': 'ک',
        'دِ': 'د',
        'بِ': 'ب',
        'زِ': 'ز',
        'ذِ': 'ذ',
        'شِ': 'ش',
        'سِ': 'س',
        'ى': 'ی',
        'ي': 'ی',
        
    }
    pattern = "|".join(map(re.escape, mapping.keys()))
    
    return re.sub(pattern, lambda m: mapping[m.group()], str(input_str))

def type_asset_mapper(assettype : str):
    if assettype=='stocks':
        return ['yellowIFB' , 'secondryIFB','funds',
                'Bourse_symbols']
    elif assettype=='stocks_and_calloptions':
        return ['yellowIFB' , 'secondryIFB','funds',
                'Bourse_symbols' , 'calloption','IFB_calloptions']
    elif assettype=='calloptions':
        return ['calloption','IFB_calloptions']
    elif assettype=='putoptions':
        return ['putoption','IFB_putoptions']
    elif assettype=='stocks_call_put':
        return type_asset_mapper('stocks') + \
    type_asset_mapper('calloptions')+type_asset_mapper('putoptions')
    else :return None
    
    
def get_all_market(assettype=None):
    ''' asset type : stocks , calloptions, 
     stocks_and_calloptions , 
     putoptions and stocks_call_put '''
    
    raw_data= market_fetcher()
    df = base_market_dataframe(raw_data)
    df2 = orderbook_dataframe(raw_data)
    df2['id'] = df2['id'].astype(str)
    df['id'] = df['id'].astype(str)
    market = df.merge(df2 ,on='id' )
    
    if assettype:
        return market[market['type_of_asset'].isin(type_asset_mapper(assettype))]
    

    return market



def extract_features(market , has_put=False, has_call=True):
    
    market['symbol']  = market['symbol'].replace({'ص.دارا':'دارا_یکم', 'ص':'پتروآگاه',
                                                                 'حافرین':'حآفرین', 'های':'های_وب',
                                                                         'هم':'هم_وزن'})
    
    
    desired_list =[]
    if has_put:
        desired_list += ['putoption','IFB_putoption']
    if has_call:
        desired_list += ['calloption','IFB_calloptions']
    if not has_call and not has_put:
        return None
        
    calloptions = market[market['type_of_asset'].isin(desired_list)]

    # calloptions = calloptions.sort_values('openInterest', ascending=False)


    def base_asset_extractor(name):
        return "_".join(re.findall(r"[\u0600-\u06FF]+", name)[1:])

    def due_date_extractor(name):
        name = name.replace('/','')
        date = name.split('-')[-1]
        if len(date)> 6:
            due_date = jdatetime.datetime.strptime(date , "%Y%m%d").date()
        else:
            due_date = jdatetime.datetime.strptime(date , "%y%m%d").date()
        
        return due_date
    def strikeprice_extractor(name):
        return int(name.split('-')[1])
        
    def base_asset_price_extractor(base_asset):
        try:
            return market['last_trade'][market['symbol']==convert_ar_characters(base_asset)].iloc[0]
        except Exception as e:            
            return None
        
    def IRR_calc(col_name):
        return (calloptions['strike_price'] / (  calloptions['base_asset_price'] - calloptions[col_name] ))**(356/calloptions['DTM']) - 1 
        

    calloptions['base_symbol']= calloptions['name'].map(base_asset_extractor)
    calloptions['jalali_due_date']= calloptions['name'].map(due_date_extractor)
    calloptions['due_date_price']= calloptions['name'].map(strikeprice_extractor)
    calloptions['base_asset_price'] = calloptions['base_symbol'].map(base_asset_price_extractor)
    calloptions['days_until_due'] = calloptions['jalali_due_date'].map(lambda x: (x - jdatetime.datetime.now().date()).days)
    calloptions['miladi_due_date'] = calloptions['jalali_due_date'].map(lambda x: x.togregorian())
    calloptions['itm'] = (calloptions['base_asset_price'] - calloptions['due_date_price']) / calloptions['base_asset_price'] 
    # calloptions['R_ask'] = IRR_calc('ask_price_1')
    return calloptions