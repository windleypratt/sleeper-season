import pandas as pd
from random import sample as sp
import http.client, urllib.request, urllib.parse
import json
from datetime import datetime, timedelta#, total_seconds


def get_transactions(
    leagues = None, 
    week = 1,
    days_back = 7,
    scoring_type = None, 
    sample = None):
    
    st = datetime.now()
    
    if leagues == None:
        al = pd.read_csv('Files/active_leagues.csv')
        al = al.dropna(subset=['type'])
        if scoring_type == None:
            al = al
        else:
            al = al[al['scoring_type'] == scoring_type]
            
        leagues = al.league_id.to_list()
        
        if sample != None:
            if sample < len(leagues):
                leagues = sp(leagues, sample)
            
        l = len(leagues)
        
        print("Getting transactions from " + str(l) + " active " + scoring_type + " league(s).")
    else:
        l = len(leagues)
        print("Getting transactions from " + str(l) + " user provided league(s).")
    
    
    all_transactions = pd.DataFrame()

    conn = http.client.HTTPSConnection('api.sleeper.app')
    
    i = 0
    start = datetime.now()
    
    for league_id in leagues:
        i += 1
        if i % 50 == 0:
            pct = (i/l)
            span = (datetime.now() - start) / pct
            print(str(i) + '/' + str(l) + ' (' + "%.2f" % (pct * 100) + '%) ETR: ' + str(start + span))
        try:
            conn.request("GET", '/v1/league/' + str(league_id) + '/transactions/' + str(1))
            trade_conn = conn.getresponse().read()
            trade = json.loads(trade_conn)

            transactions = pd.DataFrame(trade)
            #transactions['time'] = [pd.to_datetime(transactions['created'], unit='ms')]
            transactions['league_id'] = league_id
            all_transactions = all_transactions.append(transactions)
        except:
            print('Error on league ' + str(league_id))
    
    today = datetime.today()
    week_back = (today - timedelta(days=days_back))
    all_transactions['time'] = pd.to_datetime(all_transactions['created'], unit='ms')
    all_transactions = all_transactions[all_transactions['time'] > week_back]
    
    print('Finished in ' + str(datetime.now() - st) + '.')
                
    return all_transactions.reset_index(drop=True)


def get_trades(df):
    trade = df[df['type'] == 'trade']
        
    trans = trade.adds.apply(pd.Series)

    pv = pd.DataFrame()
    for index, row in trans.iterrows():
        t = pd.DataFrame(row)
        t['player_id'] = t.index
        t = t.dropna()
        t.columns = ['team', 'player_id']
        v = pd.DataFrame(t.groupby('team')['team'].transform('count').rename('value'))
        v['value'] = v['value'] / len(t['team'].drop_duplicates()) / len(t['player_id'])
        s = pd.concat([t, v], axis = 1)
        pv = pv.append(s[['player_id', 'value']].reset_index(drop=True))
        
        x = pv.groupby('player_id').mean()
        x['player_id'] = x.index
        x = x.sort_values(by='value', ascending = False).reset_index(drop=True)

        #trade_df = trade_df.sort_values(by='ratio', ascending=False).reset_index(drop=True)
        
    return x