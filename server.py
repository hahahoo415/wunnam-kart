from aiohttp import web
import asyncio, json, secrets, string, time, math, os
from pathlib import Path
rooms={}
def now(): return int(time.time()*1000)
async def broadcast(r,typ,**kw):
    players=[{k:v for k,v in p.items() if k not in ('ws','token','saved')} for p in r['players'].values()]
    for p in list(r['players'].values()):
        try:
            if p['ws'] is None: continue
            await p['ws'].send_json(dict(type=typ,players=players,id=p['id'],host=r['host'],room=r['code'],paused=r.get('paused',0),allow=r.get('allow',False),at=r['at'],duration=r['duration'],track=r.get('track','village'),now=now(),**{k:v for k,v in kw.items() if k not in ('at','duration','now')}))
        except (ConnectionError,RuntimeError,AttributeError): pass
async def index(req): return web.FileResponse(Path(__file__).with_name('index.html'))
async def ws_handler(req):
    ws=web.WebSocketResponse(heartbeat=20,max_msg_size=4096);await ws.prepare(req);r=None;pid=secrets.token_hex(8)
    async for msg in ws:
        if msg.type!=web.WSMsgType.TEXT: continue
        try: m=json.loads(msg.data);typ=m.get('type')
        except (ValueError,AttributeError): continue
        if typ in ('create','join') and r is None:
            if typ=='create':
                code=''.join(secrets.choice(string.ascii_uppercase+string.digits) for _ in range(6))
                while code in rooms: code=secrets.token_hex(3).upper()
                rooms[code]=dict(code=code,host=pid,players={},at=0,rank=0,duration=0,ended=False,pending={},paused=0,allow=False,track='village')
            else: code=str(m.get('room','')).upper()
            target=rooms.get(code)
            resume=next((p for p in target['players'].values() if p['token']==m.get('token')),None) if target and m.get('token') else None
            if not target or (not resume and (len(target['players'])>=30 or (target['at'] and (not target['allow'] or target['ended'])))):
                await ws.send_json(dict(type='error',message='입장 불가: 방 코드, 인원 또는 경기 상태 확인'));continue
            try: i=max(0,min(7,int(m.get('i',0))))
            except (ValueError,TypeError): i=0
            r=target
            if resume:
                pid=resume['id'];old=resume['ws'];resume['ws']=ws
                if old and old is not ws: await old.close()
            else: r['players'][pid]=dict(id=pid,name=str(m.get('name') or 'Racer')[:16],i=i,p=0,lane=0,rank=0,ws=ws,token=secrets.token_hex(24),saved={})
            p=r['players'][pid]
            await ws.send_json(dict(type='session',token=p['token'],id=pid,room=r['code'],host=r['host'],players=[{k:v for k,v in q.items() if k not in ('ws','token','saved')} for q in r['players'].values()],at=r['at'],now=now(),duration=r['duration'],track=r['track'],paused=r['paused'],ended=r['ended'],saved=p['saved'],allow=r['allow']))
            if not r['at']: await broadcast(r,'lobby')
        elif r and r['host']==pid and typ in ('pause','resume','stop','allow'):
            if typ=='allow': r['allow']=bool(m.get('allow'))
            elif r['at'] and not r['ended']:
                if typ=='pause' and not r['paused']: r['paused']=now()
                elif typ=='resume' and r['paused']: r['at']+=now()-r['paused']+3000;r['paused']=0
                elif typ=='stop':
                    r['ended']=True;r['paused']=0
                    for j,q in enumerate(sorted(r['players'].values(),key=lambda q:(q['rank'] or 999,-q['p'])),1):q['rank']=j
            await broadcast(r,'control',ended=r['ended'])
        elif r and r['host']==pid and ((typ=='start' and not r['at']) or (typ=='rematch' and r['at'] and all(p['rank'] for p in r['players'].values()))):
            r['track']=m.get('track') if m.get('track') in ('village','ice','santa') else r.get('track','village')
            r['at']=now()+4000
            r['rank']=0
            try: d=int(m.get('duration',0))
            except (ValueError,TypeError): d=0
            r['duration']=max(60,min(1200,d)) if d else 0
            r['ended']=False;r['paused']=0
            r['pending'].clear()
            for j,p in enumerate(r['players'].values()):
                p.update(p=0,rank=0,last_attack=0,saved={},lane=[-.65,-.22,.22,.65][j%4])
            await broadcast(r,'start',now=now(),at=r['at'],duration=r['duration'])
        elif r and not r['paused'] and typ=='attack' and r['at'] and now()>=r['at'] and not r['ended']:
            player=r['players'][pid];kind=m.get('kind');t=now()
            if player['rank'] or kind not in ('missile','fog','water','lightning') or t-player.get('last_attack',0)<1400:continue
            player['last_attack']=t
            others=[q for q in r['players'].values() if q['id']!=pid and not q['rank']]
            if kind=='fog': targets=[q for q in others if q['p']<player['p']]
            elif kind=='lightning':targets=others
            else:
                targets=[q for q in others if q['id']==m.get('target') and 0<q['p']-player['p']<=(1200 if kind=='missile' else 700)]
                if not targets:
                    await ws.send_json(dict(type='attackResult',kind=kind,ok=False));continue
                await ws.send_json(dict(type='attackResult',kind=kind,ok=True))
            for q in targets:
                try:
                    token=secrets.token_hex(8)
                    r['pending'][token]=(pid,q['id'],kind,t)
                    await q['ws'].send_json(dict(type='attack',kind=kind,owner=pid,token=token))
                except (ConnectionError,RuntimeError,AttributeError):pass
        elif r and typ=='hitAck':
            token=str(m.get('token','')); pending=r['pending'].get(token)
            if pending and pending[1]==pid and now()-pending[3]<5000:
                r['pending'].pop(token,None)
                owner=r['players'].get(pending[0]); victim=r['players'][pid]
                if owner:
                    try: await owner['ws'].send_json(dict(type='hitCam',kind=pending[2],blocked=bool(m.get('blocked')),target={k:victim[k] for k in ('id','name','i')}))
                    except (ConnectionError,RuntimeError,AttributeError): pass
        elif r and not r['paused'] and not r['ended'] and typ=='banana' and r['at'] and now()>=r['at']:
            player=r['players'][pid]
            try:
                z=float(m['z']);lane=float(m['l']); ident=str(m['id'])[:32]
                assert math.isfinite(z) and math.isfinite(lane) and abs(z-player['p'])<80 and abs(lane)<=1.35 and not player['rank']
            except (ValueError,TypeError,KeyError,AssertionError): continue
            for q in list(r['players'].values()):
                try: await q['ws'].send_json(dict(type='banana',owner=pid,z=z,l=lane,id=ident))
                except (ConnectionError,RuntimeError,AttributeError): pass
        elif r and typ=='removeBanana' and r['at']:
            for q in list(r['players'].values()):
                try: await q['ws'].send_json(dict(type='removeBanana',id=str(m.get('id',''))[:32]))
                except (ConnectionError,RuntimeError,AttributeError): pass
        elif r and not r['paused'] and typ=='state' and r['at'] and now()>=r['at']-100:
            p=r['players'][pid]
            if p['rank'] or r['ended']: continue
            try: z=float(m['p']);lane=float(m['lane']);assert math.isfinite(z) and math.isfinite(lane)
            except (ValueError,TypeError,KeyError,AssertionError): continue
            p['p']=max(p['p'],min(200000 if r['duration'] else 4800,z));p['lane']=max(-1.35,min(1.35,lane))
            p['saved']={k:m[k] for k in ('correct','attempts','boosts','charge') if isinstance(m.get(k),(int,float)) and math.isfinite(m[k])}
            if not r['duration'] and p['p']>=4800:r['rank']+=1;p['rank']=r['rank']
    if r and pid in r['players'] and r['players'][pid]['ws'] is ws:
        r['players'][pid]['ws']=None
        r['players'][pid]['disconnected']=now()
    return ws
async def tick(app):
    while True:
        await asyncio.sleep(.1)
        for r in list(rooms.values()):
            if not any(p['ws'] is not None for p in r['players'].values()) and all(now()-p.get('disconnected',now())>1800000 for p in r['players'].values()):
                rooms.pop(r['code'],None);continue
            if r['at']:
                r['pending']={k:v for k,v in r['pending'].items() if now()-v[3]<5000}
                if r['duration'] and not r['ended'] and not r['paused'] and now()>=r['at']+r['duration']*1000:
                    r['ended']=True
                    for j,p in enumerate(sorted(r['players'].values(),key=lambda p:-p['p']),1):p['rank']=j
                await broadcast(r,'snapshot',ended=r['ended'])
async def background(app):
    task=asyncio.create_task(tick(app));yield;task.cancel()
    try:await task
    except asyncio.CancelledError:pass
async def health(req): return web.Response(text='OK')
app=web.Application();app.router.add_get('/health',health);app.router.add_get('/',index);app.router.add_get('/ws',ws_handler);app.cleanup_ctx.append(background)
if __name__=='__main__':web.run_app(app,host='0.0.0.0',port=int(os.getenv('PORT','8080')))
