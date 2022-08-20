#!/usr/bin/env python
# coding: utf-8

# In[179]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:95% !important; }</style>"))
import os 
os.chdir(r'C:\Users\NaveenJoseph\Hunter Capital Limited Partnership\Firm - Documents\QUANTITATIVE\ScheduledJobs')
exec(open("Generic.py").read()) 


# In[180]:


from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

limitrows=200000000000000000000


# In[181]:


(2000*15)/60


# # Step -1 Get the Wallets Portfolio

# In[182]:



def getwalletsportfolio(x):
    chain=['eth','bsc','hmy','kcc','matic','okt','sbch'] ##  ftm - arbitrum , # avax # eliminate okt, sbch 
    alldf=[]
    for ch in chain:

        url=f'https://api.debank.com/token/balance_list?user_addr={x}&is_all=false&chain={ch}'
        print(url)
        resp = requests.get(url)
 
        d = (resp.json())
        df=pd.json_normalize(d['data'])
        if len(df)>0:
            df=df[['balance','chain','decimals','name','optimized_symbol','id','price','symbol','time_at']]
            df['balance1']=df['balance']/(10**df['decimals'])
            df['balance1']=df['price']*df['balance1']
            df['balance1']=df['balance1'].astype(float)
            df.sort_values(by=['balance1'],ascending=False,inplace=True)
            df=df.round(8)
            df=df[df['balance1']>0]
            df['wallet']=x
            df['Latest_date']= datetime.now()
            alldf.append(df)
    final_df=pd.concat(alldf)
    return final_df
x='0x1406899696adb2fa7a95ea68e80d4f9c82fcdedd'
x='0x4862733b5fddfd35f35ea8ccf08f5045e57388b3'

final_df = getwalletsportfolio(x)
final_df


# In[183]:


year_back_from_today=(datetime.today()+timedelta(days=-365)).strftime("%Y-%m-%d")
year_back_from_today=round(time.mktime(datetime.strptime(year_back_from_today, "%Y-%m-%d").timetuple()))
today=(datetime.today()+timedelta(days=0)).strftime("%Y-%m-%d")
today=round(time.mktime(datetime.strptime(today, "%Y-%m-%d").timetuple()))
threshold_days=int(today)-int(year_back_from_today)


# # Step 2 Get all of wallets Historic data 

# In[184]:



i=x

def getallhistorictrade(x,threshold_history_in_days):
    
    year_back_from_today=(datetime.today()+timedelta(days=-threshold_history_in_days)).strftime("%Y-%m-%d")
    year_back_from_today=round(time.mktime(datetime.strptime(year_back_from_today, "%Y-%m-%d").timetuple()))
    today=(datetime.today()+timedelta(days=0)).strftime("%Y-%m-%d")
    today=round(time.mktime(datetime.strptime(today, "%Y-%m-%d").timetuple()))
    threshold_days=int(today)-int(year_back_from_today)

    
    alldf=[]
    mintime=0
    dur=0
    for k in range(1000) : ## or this could be higher 
       


        url = f'https://api.debank.com/history/list?chain=eth&page_count=200&start_time={mintime}&token_id=&user_addr={i}'
        time.sleep(3)
        resp = requests.get(url)
        d = (resp.json())
        if d['data']['history_list'] and dur<=threshold_days:
            print(dur)


            df_1 = pd.json_normalize(d['data']['history_list'], 'receives', ['cate_id', 'id', 'other_addr', 'time_at'])
            
            df_2=  pd.json_normalize(d['data']['history_list'], 'sends', ['cate_id', 'id', 'other_addr', 'time_at'])
            if len(df_2)>0:
                df_2['amount']=df_2['amount']*-1
            df_1=df_1.append(df_2)
            df_1=df_1.sort_values(by=['time_at'],ascending=False)


            mintime=round(df_1['time_at'].tail(1).values[0])
            today=(datetime.today()+timedelta(days=0)).strftime("%Y-%m-%d")
            dur=round(time.mktime(datetime.strptime(today, "%Y-%m-%d").timetuple()))-mintime



            alldf.append(df_1)
            print(url)




        else:
            break 
    final_df_trade_hist=pd.concat(alldf)
    final_df_trade_hist['time_at'] = pd.to_datetime(final_df_trade_hist['time_at'], unit='s')
    return final_df_trade_hist

final_df_trade_hist=getallhistorictrade(x,400)
final_df_trade_hist


# In[187]:


final_df_trade_hist#[final_df_trade_hist['symbol']=='eth']


# In[188]:


final_df_trade_hist['time_at'].min().date()


# In[189]:


tm='1650260176'


ts = int(tm)

# if you encounter a "year is out of range" error the timestamp
# may be in milliseconds, try `ts /= 1000` in that case
print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))


# In[190]:


df_coin_saved=pd.read_csv('CoinMap.csv') # Bring all the Coins that you already have saved
df_coin_saved
final_df_trade_hist=final_df_trade_hist.merge(df_coin_saved[['token_id','symbol']],on=['token_id'],how='left')


# In[191]:


final_df_trade_hist[final_df_trade_hist['symbol'].isnull()]


# In[192]:


# final_df_trade_hist['token_id_length']=final_df_trade_hist['token_id'].apply(len)
# for idx,row in final_df_trade_hist.iterrows():
#     if (row['token_id_length']<42 and row['token_id_length']>5):
#         print('yes')
#         row['token_id']=row['id']

# final_df_trade_hist[final_df_trade_hist['token_id_length']<42]   


# In[193]:


#final_df_trade_hist.sort_values(by=['time_at'],ascending=False).head(50).to_csv('Test1.csv')


# In[194]:


final_df_trade_hist['token_id_length']=final_df_trade_hist['token_id'].apply(len)
final_df_trade_hist[final_df_trade_hist['token_id_length']<42]['token_id'].unique().tolist()


# In[195]:


# Split amount which is a qty into dollar value as of that date this will also give the $ per coin on that date 
# use cateid to convert the amount to + or - 
## Merge both frames and run iter to recreate portfolio 
# Output DF should have Date,Cointoken,$_Value,Qty,Price_Qty

def categorizer(x):
    if row['cate_id']=='receive':
        return row['amount']
    elif row['cate_id']=='send':
        return row['amount']*-1
    else:
        return 0
    

url = 'https://etherscan.io/tx/0xc0818c6b8cbef35b4eaff00b70b11db239b2a4f7a8593babe8f5ff33e9036a59' ## type Swap page 
headers = {'User-Agent': 'My User Agent 1.0','From': 'youremail@domain.example'}  # This is another valid field}



def gettnvalueoftransaction_hash(hasvalue):
    url=f'https://etherscan.io/tx/{hasvalue}'
    print(url)
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    elem=soup.find('button', attrs={"id": "tokenpricebutton"})
    if elem:
        elem=elem.attrs['value']
        elem=elem.replace("(","").replace(")","").replace("$","").replace(",","").strip()
        elem=float(elem)
    else:
        elem=0
    
    tnelem=soup.find('button',attrs={"id":'txfeebutton'})
    if tnelem:
        tnelem=tnelem.string.replace("(","").replace(")","").replace("$","").replace(",","").strip()
        tnelem=float(tnelem)
    else:
        tnelem=0
    gas_elem=soup.find('span', attrs={"id": "ContentPlaceHolder1_spanGasPrice"})
    if gas_elem:
        
        gas_elem=''.join(gas_elem.strings)
        gas_elem=gas_elem.replace("Gwei","").replace(",","").strip().split(" ")[0]
        gas_elem=float(gas_elem)
    else:
        gas_elem=0
        
    print(elem,tnelem,gas_elem)

    return elem,tnelem,gas_elem
           
def getgasvalueoftn(hasvalue):
    url=f'https://etherscan.io/tx/{hasvalue}'
    #print(url)
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
  
    gas_elem=soup.find('span', attrs={"id": "ContentPlaceHolder1_spanGasPrice"})
    if gas_elem:
        
        gas_elem=''.join(gas_elem.strings)
        gas_elem=gas_elem.replace("Gwei","").replace(",","").strip().split(" ")[0]
        gas_elem=float(gas_elem)
    else:
        gas_elem=0
        
    #print(gas_elem)

    return gas_elem

    
#tn_value,tn_cost,gas=gettnvalueoftransaction_hash('0xc0818c6b8cbef35b4eaff00b70b11db239b2a4f7a8593babe8f5ff33e9036a59')
#getgasvalueoftn('0xc0818c6b8cbef35b4eaff00b70b11db239b2a4f7a8593babe8f5ff33e9036a59')
#tn_value,tn_cost,gas

def getsymbol_of_tok(tok):
    url=f'https://etherscan.io/token/{tok}'
    print(tok)
    time.sleep(3)
    response = requests.get(url, headers=headers)
  
    soup = BeautifulSoup(response.text, 'html.parser')
    k=soup.find('input', attrs={"id": "ContentPlaceHolder1_hdnSymbol"})
    try:
        print(k.attrs['value'])
        return k.attrs['value']
        
    except:
        pass
        return 'FAKE_TOKEN'
        
def get_coin_from_tx(tok):

   
    url=f'https://etherscan.io/tx/{tok}'
  

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    k=soup.find('span',attrs={"id": "spanToAdd"})
    try:
        if k:
            print(k.text)
            symbol=getsymbol_of_tok(k.text)
     
            return symbol
        else:
            return 'FAKE_TOKEN'
    except:
        pass
        return 'FAKE_TOKEN'
        

    

def getdate(x):
    return datetime.utcfromtimestamp(int(x)).strftime('%Y-%m-%d')


def getpriceofcoin(symbol,date):

    
    #try:
    url=f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym=USD&limit=365'
    resp=requests.get(url)
    if resp.json()['Response']=='Success':
        df_price=pd.DataFrame(resp.json()['Data']['Data'])
        df_price['Date'] =df_price['time'].apply(getdate)
        return df_price[df_price['Date']==date]['close'].values[0]
    else:
        return 0
        
def getdailypriceofcoin(symbol):

    
    #try:
    url=f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym=USD&limit=365'
    resp=requests.get(url)
    if resp.json()['Response']=='Success':
        df_price=pd.DataFrame(resp.json()['Data']['Data'])
        df_price['Date'] =df_price['time'].apply(getdate)
        df_price=df_price[['Date','close']]
        return df_price
    else:
        return None 
#getpriceofcoin('BOSS','2022-01-01')        


#getsymbol_of_tok('0x35fe8bc3008ca8ed5395f7a576cd3b194e627017')
    


# In[196]:


final_df_trade_hist


# In[197]:


final_df_trade_hist_reduced=final_df_trade_hist.head(limitrows)


# In[198]:


final_df_trade_hist[(final_df_trade_hist['symbol'].isnull())&(final_df_trade_hist['token_id_length']==42)]['token_id'].unique().tolist()


# In[199]:


df_coin_map=pd.DataFrame(final_df_trade_hist[(final_df_trade_hist['symbol'].isnull())&(final_df_trade_hist['token_id_length']==42)]['token_id'].unique().tolist())
df_coin_map#.columns=['token_id']


# In[200]:


## Recreating the coinmap
df_coin_map=pd.DataFrame(final_df_trade_hist[(final_df_trade_hist['symbol'].isnull())&(final_df_trade_hist['token_id_length']==42)]['token_id'].unique().tolist())
if len(df_coin_map)>0:
    df_coin_map.columns=['token_id']
    df_coin_map['token_id']=df_coin_map['token_id'].str.strip()
    df_coin_map['symbol']=df_coin_map['token_id'].apply(getsymbol_of_tok)
    df_coin_map


# In[201]:


df_coin_map


# In[202]:


final_df_trade_hist_reduced[final_df_trade_hist_reduced['symbol'].isnull()]


# In[203]:


df_coin_map2=pd.DataFrame(final_df_trade_hist_reduced[(final_df_trade_hist_reduced['token_id_length']<42)&(final_df_trade_hist_reduced['token_id_length']>5)& (final_df_trade_hist_reduced['symbol'].isnull())  ]['id'].unique().tolist())
df_coin_map2


# In[204]:


if len(df_coin_map2)>0:
    df_coin_map2.columns=['token_id']
    df_coin_map2['token_id']=df_coin_map2['token_id'].str.strip()
    df_coin_map2=df_coin_map2.head(10)
    df_coin_map2['symbol']=df_coin_map2['token_id'].apply(get_coin_from_tx)
    df_coin_map2

df_coin_map2


# In[205]:


df_coin_map2


# In[206]:


#df_coin_map2[df_coin_map2['symbol']!='FAKE_TOKEN']


# In[207]:


df_coin_map=df_coin_map.append(df_coin_map2)
df_coin_map


# In[208]:


df_coin_map=df_coin_saved.append(df_coin_map)
df_coin_map


# In[209]:


df_coin_map.to_csv('CoinMap.csv')


# In[210]:


def getcoinsymbol_liveLookup(tok):
    try:
        return df_coin_map[df_coin_map['token_id']==tok]['symbol'].head(1).values[0]
    except:
        return None


# In[211]:


getcoinsymbol_liveLookup(tok='0x695d4e4936c0de413267c75ca684fc317e03a819')


# In[212]:


final_df_trade_hist_reduced.drop(columns=['symbol'],inplace=True)


# In[213]:


final_df_trade_hist_reduced=final_df_trade_hist_reduced.merge(df_coin_map[['token_id','symbol']],on=['token_id'],how='left')


# In[214]:


for idx,row in df_coin_map.iterrows():
    if row['token_id']=='eth':
        row['symbol']='eth'
    elif row['token_id']=='bsc':
        row['symbol']='bsc'
df_coin_map


# In[215]:


# all_symbol=[]
# all_coin=[]
# for coin in final_df_trade_hist_reduced['token_id'].unique().tolist():
# #     print(coin)
#     symbol=getsymbol_of_tok(coin)
# #     print(symbol)
#     all_symbol.append(symbol)
#     all_coin.append(coin)
# df_coin_map=pd.DataFrame()
# df_coin_map['symbol']=all_symbol
# df_coin_map['token_id']=all_coin
# df_coin_map


# In[216]:


getsymbol_of_tok('0xdac17f958d2ee523a2206206994597c13d831ec7')


# In[217]:


df_coin_map[df_coin_map['symbol']=='FAKE_TOKEN']


# In[218]:


df_coin_map['symbol'].value_counts()


# In[219]:


#final_df_trade_hist_reduced.drop(columns=['symbol_x','symbol_y'],inplace=True)


# In[220]:


final_df_trade_hist_reduced


# In[221]:


#final_df_trade_hist_reduced=final_df_trade_hist_reduced.merge(df_coin_map,on=['token_id'])
final_df_trade_hist_reduced=final_df_trade_hist_reduced[final_df_trade_hist_reduced['symbol']!='FAKE_TOKEN']
final_df_trade_hist_reduced


# In[222]:


len(final_df_trade_hist_reduced)


# In[223]:


final_df_trade_hist_reduced['time_at']=final_df_trade_hist_reduced['time_at'].dt.strftime('%Y-%m-%d')


# In[224]:


len(final_df_trade_hist_reduced['symbol'].unique().tolist())


# In[225]:


## get all the 133 coins into a dataframe and keep it there 
alldf=[]
for coin in final_df_trade_hist_reduced['symbol'].unique().tolist():
    if coin not in ['FAKE_TOKEN']:
        df=getdailypriceofcoin(coin)
        
        if df is not None and len(df)>0:
            df['symbol']=coin
            alldf.append(df)
final_price_df=pd.concat(alldf)
final_price_df


# In[ ]:





# In[226]:


# all_price=[]
# all_symbol=[]
# all_date=[]
# for idx,row in final_df_trade_hist_reduced[['time_at','symbol']].drop_duplicates().iterrows():
# #     print(row['symbol'],row['time_at'])
#     price=getpriceofcoin(row['symbol'],row['time_at'])
#     all_price.append(price)
#     all_symbol.append(row['symbol'])
#     all_date.append(row['time_at'])

# df_coin_price=pd.DataFrame()
# df_coin_price['symbol']=all_symbol
# df_coin_price['time_at']=all_date
# df_coin_price['price']=all_price
# df_coin_price
#final_df_trade_hist_reduced['price']=all_price
# final_df_trade_hist_reduced


# In[227]:


final_df_trade_hist_reduced


# In[228]:


final_price_df.rename(columns={'Date':'time_at'},inplace=True)


# In[229]:


final_df_trade_hist_reduced=final_df_trade_hist_reduced.merge(final_price_df,on=['symbol','time_at'])


# In[230]:


final_df_trade_hist_reduced=final_df_trade_hist_reduced[final_df_trade_hist_reduced['close']!=0]


# In[232]:


try:
    final_df_trade_hist_reduced['gas_price']=final_df_trade_hist_reduced['id'].apply(getgasvalueoftn)
except:
    pass
    final_df_trade_hist_reduced['gas_price']=99
    


# In[234]:


final_df_trade_hist_reduced.rename(columns={'amount':'Trade_Qty','time_at':'Date'},inplace=True)


# In[235]:


df_wallet_trade=final_df_trade_hist_reduced[['Date','token_id','Trade_Qty']]


# In[236]:


#[['Date','token_id','Start_Qty','Trade_Qty','End_Qty']]
final_df.rename(columns={'balance':'Start_Qty','Latest_date':'Date','id':'token_id'},inplace=True)
final_df['Date']=final_df['Date'].dt.strftime('%Y-%m-%d')


# In[237]:


df_port=final_df[['Date','token_id','Start_Qty']]


# In[238]:


#df_union_all=
df_union_all=df_port[['Date','token_id','Start_Qty']].append(df_wallet_trade[['Date','token_id','Trade_Qty']]).fillna(0)


# In[239]:


end=datetime.today().strftime('%Y-%m-%d')
start=(datetime.today()+timedelta(days=-365)).strftime('%Y-%m-%d')


# In[240]:


#Create a template table 

df_datelist=pd.DataFrame()
df_datelist['Date']=pd.date_range(start=start,end=end).to_pydatetime().tolist()
df_datelist

df_datelist['key']=0
df_all=df_union_all[['token_id']]
df_all['key']=0


# In[241]:


df_template=df_all.merge(df_datelist, on='key', how='outer') ## this will have all the unique coins for every day 


# In[242]:


df_template['Date']=df_template['Date'].dt.strftime('%Y-%m-%d')


# In[243]:


df_wallet_trade[df_wallet_trade['token_id']=='eth']


# In[244]:


df_template_trade=df_template.merge(df_wallet_trade[['Date','token_id','Trade_Qty']],on=['Date','token_id'],how='left').fillna(0) 


# In[245]:


df_template_trade.drop_duplicates(inplace=True)


# In[246]:


df_template_trade[(df_template_trade['token_id']=='eth')&(df_template_trade['Date']=='2022-05-26')]


# In[247]:


#df_all_template_union[df_all_template_union['Start_Qty']>0]


# In[248]:


df_template_port=df_template.merge(df_port[['Date','token_id','Start_Qty']],on=['Date','token_id'],how='left')


# In[249]:


df_port


# In[250]:


df_template_trade=df_template_trade.groupby(['token_id','key','Date'])[['Trade_Qty']].sum().reset_index()


# In[251]:


df_template_trade[(df_template_trade['token_id']=='eth')&(df_template_trade['Date']=='2022-05-26')]


# In[252]:


####restart from here 


# In[253]:


df_all_template_union=df_template_port.append(df_template_trade).sort_values(by=['Start_Qty'],ascending=False)


# In[254]:


df_all_template_union


# In[255]:


# df_all_template_union
#df_all_template_union.groupby(['token_id','key','Date'])[['Start_Qty','Trade_Qty']].sum().reset_index()


# In[256]:


# break
df_all_template_union.drop_duplicates(inplace=True)


# In[257]:


df_all_template_union[df_all_template_union['Start_Qty']>0]


# In[258]:


#df_all_template_union=df_all_template_union[df_all_template_union['Date']!=end] ## I am not sure if this needs to exist 


# In[259]:


df_all_template_union.fillna(0,inplace=True)


# In[260]:


df_all_template_union['Start_Qty']=pd.to_numeric(df_all_template_union['Start_Qty'],errors='coerce')


# In[261]:


df_all_template_union_bkup=df_all_template_union


# In[262]:


six_dec_coins=df_coin_map[df_coin_map['symbol'].isin(['USDC','USDT'])]['token_id'].unique().tolist()


# In[263]:


def applydecimalcalc(row):
    if row['token_id'] in six_dec_coins:
        row['Start_Qty']=row['Start_Qty']/1000000
#         row['Trade_Qty']=row['Trade_Qty']/1000000
    else:
        row['Start_Qty']=row['Start_Qty']/1000000000000000000
#         row['Trade_Qty']=row['Trade_Qty']/1000000000000000000
    return row

        
        


# In[264]:


df_all_template_union=df_all_template_union.apply(applydecimalcalc,axis=1)


# In[265]:


df_all_template_union


# In[266]:


len(df_all_template_union)


# In[267]:


df_all_template_union[df_all_template_union['Date']=='2022-07-04']


# In[268]:


df_all_template_union[df_all_template_union['token_id']=='0x25f8087ead173b73d6e8b84329989a8eea16cf73']


# In[269]:


# df_all_template_union_bkup[df_all_template_union_bkup['Trade_Qty']>0]


# In[270]:


# tok='0xdac17f958d2ee523a2206206994597c13d831ec7'
# df_test_single_dummy=df_all_template_union[df_all_template_union['token_id']==tok]




# df_test_single_dummy=df_test_single_dummy.groupby(['token_id','key','Date'])[['Start_Qty','Trade_Qty']].sum().reset_index()
# df_test_single_dummy=df_test_single_dummy.sort_values(by=['Date'],ascending=False)
# df_test_single_dummy=df_test_single_dummy.reset_index()
# df_test_single_dummy=df_test_single_dummy.drop(columns=['index'])


# In[271]:


# df_test_single_dummy


# In[272]:


#df_test_single_dummy[df_test_single_dummy['Date']=='2022-07-03']


# In[273]:


# all1=[]
# for row in df_test_single_dummy.itertuples():
#     if row.Index>0:
#         if row[5]==0:

#             following_day_start_qty=df_test_single_dummy['Start_Qty'].iloc[row.Index-1]
#             df_test_single_dummy.at[row.Index,"Start_Qty"]=following_day_start_qty
#         elif row[5]!=0:
# #             print('found')
# #             print(df_test_single_dummy['Start_Qty'].iloc[row.Index-1])
# #             print(row[5])
# #             break
#             current_day_start_qty=df_test_single_dummy['Start_Qty'].iloc[row.Index-1]-row[5] # following day start qty - todays trade qty
#             df_test_single_dummy.at[row.Index,"Start_Qty"]=current_day_start_qty
# df_test_single_dummy


# In[274]:


#df_test_single_dummy[df_test_single_dummy['token_id']=='0xdac17f958d2ee523a2206206994597c13d831ec7'].sort_values(by=['Date'],ascending=False).head(50).to_csv('Output.csv')


# In[275]:


# df_test_single_dummy[df_test_single_dummy['Trade_Qty']!=0]
#df_portfolio_all_day[df_portfolio_all_day['token_id']=='0xdac17f958d2ee523a2206206994597c13d831ec7'].sort_values(by=['Date'],ascending=False).head(50)


# In[276]:


all_df=[]
for tok in df_all_template_union['token_id'].unique().tolist():

   
    
    df_test_single_dummy=df_all_template_union[df_all_template_union['token_id']==tok]




    df_test_single_dummy=df_test_single_dummy.groupby(['token_id','key','Date'])[['Start_Qty','Trade_Qty']].sum().reset_index()
    df_test_single_dummy=df_test_single_dummy.sort_values(by=['Date'],ascending=False)
    df_test_single_dummy=df_test_single_dummy.reset_index()
    df_test_single_dummy=df_test_single_dummy.drop(columns=['index'])
    

    
    
    all1=[]
    for row in df_test_single_dummy.itertuples():
        if row.Index>0:
            if row[5]==0:

                following_day_start_qty=df_test_single_dummy['Start_Qty'].iloc[row.Index-1]
                df_test_single_dummy.at[row.Index,"Start_Qty"]=following_day_start_qty
            elif row[5]!=0:
#                 print('found')
                current_day_start_qty=df_test_single_dummy['Start_Qty'].iloc[row.Index-1]-row[5] # following day start qty - todays trade qty
                df_test_single_dummy.at[row.Index,"Start_Qty"]=current_day_start_qty
    
    all_df.append(df_test_single_dummy)


df_portfolio_all_day=pd.concat(all_df)
df_portfolio_all_day


# In[277]:


df_portfolio_all_day.drop_duplicates(inplace=True)


# In[278]:


df_portfolio_all_day[df_portfolio_all_day['Start_Qty']>0]['Date'].unique().tolist() ## gives > 50 K rows or allmost all the 365 rows


# In[279]:


df_portfolio_all_day[df_portfolio_all_day['Trade_Qty']!=0]


# In[280]:


#df_portfolio_all_day[df_portfolio_all_day['token_id']=='0xdac17f958d2ee523a2206206994597c13d831ec7'].sort_values(by=['Date'],ascending=False).drop_duplicates().to_csv('Output.csv')


# In[281]:


df_portfolio_all_day['token_id'].unique().tolist()
## Now get the coin symbol and also get the last 365 days price 


# In[282]:


df_portfolio_all_day[(df_portfolio_all_day['token_id']=='eth')&(df_portfolio_all_day['Date']=='2022-05-26')]


# In[283]:


all_token_id=[]
all_symbol=[]
for coin in df_portfolio_all_day['token_id'].unique().tolist():
    token_id=coin
    all_token_id.append(coin)
    #all_symbol.append(getsymbol_of_tok(coin))
    all_symbol.append(getcoinsymbol_liveLookup(coin)) ## this gets you from the already saved database


# In[284]:


df_coin_actual_map=pd.DataFrame()
df_coin_actual_map['symbol']=all_symbol
df_coin_actual_map['token_id']=all_token_id
df_coin_actual_map


# In[285]:


for idx,row in df_coin_actual_map.iterrows():
    if row['token_id']=='eth':
        row['symbol']='eth'
    elif row['token_id']=='bsc':
        row['symbol']='bsc'
df_coin_actual_map


# In[286]:


df_portfolio_all_day=df_portfolio_all_day.merge(df_coin_actual_map,on=['token_id'],how='left')


# In[287]:


df_portfolio_all_day


# In[288]:


df=getdailypriceofcoin('eth')


# In[289]:


df_portfolio_all_day['symbol'].unique().tolist()


# In[290]:


alldf=[]
for coin in df_portfolio_all_day['symbol'].unique().tolist():
    if coin not in ['FAKE_TOKEN'] :
        print(coin)
        df=getdailypriceofcoin(coin)
        if df is not None:
            df['symbol']=coin
            df=df[['Date','symbol','close']]
            alldf.append(df)
final_df_price=pd.concat(alldf)        
final_df_price        


# In[291]:


df_portfolio_all_day=df_portfolio_all_day.merge(final_df_price,on=['Date','symbol'])


# In[292]:


final_df_trade_hist['time_at'].min().date().strftime('%Y-%m-%d')


# In[293]:


df_portfolio_all_day=df_portfolio_all_day[df_portfolio_all_day['Date']>final_df_trade_hist['time_at'].min().date().strftime('%Y-%m-%d')]


# In[294]:


(df_portfolio_all_day[['symbol','Date','close','Start_Qty','Trade_Qty','token_id']]).to_csv('Final_bfilled_output.csv')


# In[295]:


df_portfolio_all_day[df_portfolio_all_day['Trade_Qty']>0]

