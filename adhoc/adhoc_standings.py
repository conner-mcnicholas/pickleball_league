import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime

sa = gspread.service_account()
sh = sa.open("SCALPEL Resources (DOUBLES)")

schedule_ws = sh.worksheet("SCHEDULE")

schedule = get_as_dataframe(schedule_ws,nrows=109)[['Match','Week','Leg', \
    'T-A','T-B','Player A-1','Player A-2','Player B-1','Player B-2', \
    '1A','1B','2A','2B','3A','3B']]


played = schedule[pd.notna(schedule['1A'])]

dr={'M':{1:[0,0],2:[0,0],3:[0,0],4:[0,0],5:[0,0],6:[0,0],7:[0,0],8:[0,0],9:[0,0]},
    'G':{1:[0,0],2:[0,0],3:[0,0],4:[0,0],5:[0,0],6:[0,0],7:[0,0],8:[0,0],9:[0,0]},
    'P':{1:[0,0],2:[0,0],3:[0,0],4:[0,0],5:[0,0],6:[0,0],7:[0,0],8:[0,0],9:[0,0]}}

for m in range(len(played)):
    match = played.iloc[m]

    matchnum = match['Match']
    A,B = match[['T-A','T-B']]
    G1A,G1B,G2A,G2B,G3A,G3B = match[['1A','1B','2A','2B','3A','3B']]
    
    #print(f'\nmatch #{matchnum}:')
    #print(f'1A:{G1A},1B:{G1B}\n2A:{G2A},2B:{G2B}\n3A:{G3A},3B:{G3B}')
    PA = pd.Series([G1A,G2A,G3A]).sum().astype(int)
    PB = pd.Series([G1B,G2B,G3B]).sum().astype(int)
    if pd.isna(G3A):
        #print('settled in 2 games')
        if G2A > G2B:
            #print(f'{A} beat {B} 2 games to 0')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][B][1]+=2
        else:
            #print(f'{B} beat {A} 2 games to 0')
            dr['M'][A][1]+=1
            dr['M'][B][0]+=1
            dr['G'][A][1]+=2
            dr['G'][B][0]+=2
    else:
        #print('settled in 3 games')
        if G3A > G3B:
            #print(f'{A} beat {B} 2 games to 1')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][A][1]+=1
            dr['G'][B][1]+=2
            dr['G'][B][0]+=1
        else:
            #print(f'{B} beat {A} 2 games to 1')
            dr['M'][B][0]+=1
            dr['M'][A][1]+=1
            dr['G'][B][0]+=2
            dr['G'][B][1]+=1
            dr['G'][A][1]+=2
            dr['G'][A][0]+=1
    #print(f'Team {A} scored {PA} points\nTeam {B} scored {PB} points')
    dr['P'][A]=[dr['P'][A][0]+PA,dr['P'][A][1]+PB]
    dr['P'][B]=[dr['P'][B][0]+PB,dr['P'][B][1]+PA]

df_standings = pd.DataFrame(Counter(pd.concat([played['T-A'],played['T-B']])).items(),columns=['Tnum','MP']).sort_values('Tnum')
df_standings['Team'] = ['Deathballers (T-1)','The Ass Paddlers (T-2)','Bert & Erne (T-3)','The Lobbyists (T-4)','Brommer St. Bangers (T-5)','SEAL Team 6 (T-6)', \
    'The Lucky 777s (T-7)','NADS (T-8)','The Ball Slammers (T-9)']

df_standings['MW']=[dr['M'][x][0] for x in df_standings.Tnum]
df_standings['ML']=[dr['M'][x][1] for x in df_standings.Tnum]
df_standings['MR']=(df_standings.MW/df_standings.MP).round(4)

df_standings['GW']=[dr['G'][x][0] for x in df_standings.Tnum]
df_standings['GL']=[dr['G'][x][1] for x in df_standings.Tnum]
df_standings['GP']=df_standings[['GW','GL']].sum(axis = 1, skipna = True)
df_standings['GR']=(df_standings.GW/df_standings.GP).round(4)

df_standings['PF']=[dr['P'][x][0] for x in df_standings.Tnum]
df_standings['PA']=[dr['P'][x][1] for x in df_standings.Tnum]
df_standings['PD']=(df_standings.PF-df_standings.PA)
df_standings['PR']=(df_standings.PF/(df_standings.PF+df_standings.PA)).round(4)

#df_standings = df_standings.sort_values(['MR','MW','GR','GW','PR','PF'],ascending=[False,False,False,False,False,False])

df_standings['#']=range(1,10)
df_standings = df_standings[['#','Team','MP','MW','ML','MR','GP','GW','GL','GR','PF','PA','PD','PR']]
print(df_standings.reset_index(drop=True).to_string())

current_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
f = open("/Users/conner/pickleball_league/updatelog.txt", "a")
f.write(f'{len(played)} matches in standings | UPDATING ADHOC | {current_ts}\n')
standings_ws = sh.worksheet("STANDINGS")
set_with_dataframe(standings_ws, df_standings, row=3, col=2)
