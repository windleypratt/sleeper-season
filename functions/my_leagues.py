from datetime import datetime, timedelta
import pandas as pd
from random import sample as sp
import json
import http.client, urllib.request, urllib.parse

leagues = {'ons' : 720025558011953152,
          'whiskey' : 725794116117590016}

def get_league(key = 'ons', league_id = None):
    if league_id != None:
        s = str(league_id)
    else:
        s = str(leagues.get(key))
        
    conn = http.client.HTTPSConnection('api.sleeper.app')

    try:
        conn.request("GET", '/v1/league/' + str(s) + '/users')
        start_conn = conn.getresponse().read()
        ls = json.loads(start_conn)

        lineups = pd.DataFrame(ls)

    except Exception as e:
        raise(e)

    return lineups
    
def get_league_rosters(key = 'ons', league_id = None):
    if league_id != None:
        s = str(league_id)
    else:
        s = str(leagues.get(key))
        
    conn = http.client.HTTPSConnection('api.sleeper.app')

    try:
        conn.request("GET", '/v1/league/' + str(s) + '/rosters')
        start_conn = conn.getresponse().read()
        rs = json.loads(start_conn)

        rosters = pd.DataFrame(rs)

    except Exception as e:
        raise(e)

    return rosters

def get_drafts_from_league(key = 'ons', league_id = None):
    if league_id != None:
        s = str(league_id)
    else:
        s = str(leagues.get(key))
        
    l = get_league(league_id = s)
    
    users = l.user_id.to_list()
    
    conn = http.client.HTTPSConnection('api.sleeper.app')
    
    all_drafts = pd.DataFrame()
    
    print(users)
    
    for user_id in users:
        
        try:
            conn.request("GET", "/v1/user/" + str(user_id) + '/drafts/nfl/2022')
            response = conn.getresponse().read()
            jsonResponse = json.loads(response)

            drafts = pd.json_normalize(jsonResponse)
            drafts['user'] = user_id
            
            all_drafts = all_drafts.append(drafts)

            #drafts = drafts[['type', 'status','start_time','season_type','season','league_id','draft_id','settings.teams','metadata.scoring_type']]

        except:
            pass
        
    conn.close()
        
    return all_drafts
    
