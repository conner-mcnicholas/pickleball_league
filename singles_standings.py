import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime

sa = gspread.service_account()
sh = sa.open("SCALPEL Resources (SINGLES)")

schedule_ws = sh.worksheet("SCHEDULE")

schedule = get_as_dataframe(schedule_ws,nrows=90)[['Match','Week', \
    'Player A','Player B','1A','1B','2A','2B','3A','3B']]

played = schedule[pd.notna(schedule['1A'])]

current_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
f = open("/Users/conner/pickleball_league/singles_updatelog.txt", "r+")
s=f.readlines()[-1]
prev_mp = int(s[:s.find(' ')])
print(f'standings reflects {len(played)} matches played, compared to previous run of {prev_mp}')
if prev_mp == len(played):
    f.write(f"{len(played)} matches already accounted for | QUITTING | {current_ts}\n")
    quit()

dr={'M':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]},
    'G':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]},
    'P':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]}}
    
for m in range(len(played)):
    match = played.iloc[m]

    matchnum = match['Match']
    A,B = match[['Player A','Player B']]
    G1A,G1B,G2A,G2B,G3A,G3B = match[['1A','1B','2A','2B','3A','3B']]
    
    print(f'\nmatch {matchnum}:')
    print(f'1A:{G1A},1B:{G1B}\n2A:{G2A},2B:{G2B}\n3A:{G3A},3B:{G3B}')
    PA = pd.Series([G1A,G2A,G3A]).sum().astype(int)
    PB = pd.Series([G1B,G2B,G3B]).sum().astype(int)
    if pd.isna(G3A):
        print('settled in 2 games')
        if G2A > G2B:
            print(f'{A} beat {B} 2 games to 0')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][B][1]+=2
        else:
            print(f'{B} beat {A} 2 games to 0')
            dr['M'][A][1]+=1
            dr['M'][B][0]+=1
            dr['G'][A][1]+=2
            dr['G'][B][0]+=2
    else:
        print('settled in 3 games')
        if G3A > G3B:
            print(f'{A} beat {B} 2 games to 1')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][A][1]+=1
            dr['G'][B][1]+=2
            dr['G'][B][0]+=1
        else:
            print(f'{B} beat {A} 2 games to 1')
            dr['M'][B][0]+=1
            dr['M'][A][1]+=1
            dr['G'][B][0]+=2
            dr['G'][B][1]+=1
            dr['G'][A][1]+=2
            dr['G'][A][0]+=1
    print(f'{A} scored {PA} points\n{B} scored {PB} points')
    dr['P'][A]=[dr['P'][A][0]+PA,dr['P'][A][1]+PB]
    dr['P'][B]=[dr['P'][B][0]+PB,dr['P'][B][1]+PA]

df_standings = pd.DataFrame(Counter(pd.concat([played['Player A'],played['Player B']])).items(),columns=['Player','MP']).sort_values('Player')

df_standings['MW']=[dr['M'][x][0] for x in df_standings.Player]
df_standings['ML']=[dr['M'][x][1] for x in df_standings.Player]
df_standings['MR']=(df_standings.MW/df_standings.MP).round(4)

df_standings['GW']=[dr['G'][x][0] for x in df_standings.Player]
df_standings['GL']=[dr['G'][x][1] for x in df_standings.Player]
df_standings['GP']=df_standings[['GW','GL']].sum(axis = 1, skipna = True)
df_standings['GR']=(df_standings.GW/df_standings.GP).round(4)

df_standings['PF']=[dr['P'][x][0] for x in df_standings.Player]
df_standings['PA']=[dr['P'][x][1] for x in df_standings.Player]
df_standings['PD']=(df_standings.PF-df_standings.PA)
df_standings['PR']=(df_standings.PF/(df_standings.PF+df_standings.PA)).round(4)

df_standings = df_standings.sort_values(['MR','MW','GR','GW','PR','PF'],ascending=[False,False,False,False,False,False])
df_standings['Rank']=range(1,1+len(df_standings))
df_standings = df_standings[['Rank','Player','MP','MW','ML','MR','GP','GW','GL','GR','PF','PA','PD','PR']]
print(df_standings.reset_index(drop=True).to_string())

f.write(f'{len(played)} matches now in standings | EDITING SHEET | {current_ts}\n')
standings_ws = sh.worksheet("STANDINGS")
set_with_dataframe(standings_ws, df_standings, row=2, col=2)
