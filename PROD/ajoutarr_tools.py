#-*- coding: iso-8859-15 -*-

from sys import exit, argv
import os
import sqlalchemy
from re import match,search
import datetime
from time import mktime
from pytz import UTC

from numpy import log,pi
Rearth=6371.0
km2deg=180/(pi*Rearth)
deg2km=(pi*Rearth)/180

#--------------------------------------------------------------------
global DEBUG
DEBUG=0

def TIMESTAMP_TO_DATETIME (timestamp): # -> YYYY-MM-DD HH:MM:SS.S
    A=datetime.datetime.utcfromtimestamp(timestamp+0.05)
    return A

def valid_date(datestring):
    try:
        datetime.datetime.strptime(datestring[:8], '%H:%M:%S')
        return True
    except ValueError:
        return False

def connect_dtb(engine):
    try:
        oracle_db=sqlalchemy.create_engine(engine)
    except:
        print "ERROR: No Oracle access."
        raise
    
    print "Access to Oracle database (%s) OK !" % engine
    conn = oracle_db.connect()
    return conn

def close_dtb(conn):
    conn.close()

class Arrival():
    def __init__(self, conn, otime, otime_unix, auth):
        self.stacode=''
        self.dist=0
        self.azim=0
        self.phase=''
        self.arrtime=0
        self.Sarrtime=0
        self.chanid=0
        self.chan='CPZ'
        self.commid=1101
        self.stype='-'
        self.qual='E'
        self.conn=conn
        self.otime=otime
        self.otime_unix=otime_unix
        self.auth=auth
        self.weight=1.0

    def load_bull_line(self,l):
        global isP
        global mysta

        # -----------
        if (self.auth == 'GFZ'):
            self.stacode,self.dist=l.split()[0].strip(),float(l.split()[7])
            self.phase='P'
            
            self.arrtime=l.split()[2]+" "+l.split()[3]+"00"
            self.arrtime= datetime.datetime.strptime(self.arrtime, '%y/%m/%d %H:%M:%S.%f')
            self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
        
        # -----------
        elif (self.auth == 'BGS'):
            self.stacode, self.arrtime, self.phase=l.split()[1].strip(),l.split()[2]+" "+l.split()[3],l.split()[4].strip()
            #print self.stacode, self.phase, self.arrtime
            self.arrtime= datetime.datetime.strptime(self.arrtime, '%Y/%m/%d %H:%M:%S.%f')
            self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
            #print self.stacode, self.phase, self.arrtime
        
		# -----------
        elif (self.auth == 'SED'):
            self.stacode, self.dist, self.azim, self.phase, self.arrtime=l.split()[0].strip(),l.split()[1],l.split()[2],l.split()[3].strip(),l.split()[4]+"00"
        # -----------
        elif (self.auth == 'SC3'):
            self.phase, self.stacode, self.dist,  self.arrtime=l.split(";")[2].strip(),l.split(";")[8].strip(),float(l.split(";")[11]),l.split(";")[13]+"00"
        # -----------
        elif (self.auth == 'OCA_M'):
            self.stacode, self.arrtime=l.split()[0].strip(),l.split()[3]+"00"

            #print ">> isP=",isP, self.stacode,mysta
            # Si même station que la précédente, alors c'est une phase S
            # Si station différente de la précédente, alors c'est une phase P
            if (self.stacode==mysta):
                if (isP==1):
                    self.phase="Pg"
                    isP=0
                else:
                    self.phase="Sg"
                    isP=1
            else:
                self.phase="Pg"
                isP=0
                mysta=self.stacode
            
        # -----------
        elif (self.auth == 'RNS'):
            self.stacode, self.phase, self.arrtime=l.split()[1].strip(),l.split()[2].strip(),l.split()[3][:10]

        # -----------
        elif (self.auth == 'KOLN'):
            l=l[:28]
            
            try:
                self.stacode=l.split()[0].strip()
                self.phase=l.split()[2].strip()
            except:
                return
            
            if ((self.phase=="EP") | (self.phase=="IP")):
                self.phase="Pg"
            elif ((self.phase=="ES") | (self.phase=="IS")):
                self.phase="Sg"
            else:
                return

            #ls=l.split("    ")
            l=l.replace(" 0 ","   ")
            l=l.replace(" 1 ","   ")
            l=l.replace(" C ","   ")
            l=l.replace(" D ","   ")
            
            ls=l.split("    ")
            #print ls
            #ls[1].strip().replace(" ","0")
            
            #self.arrtime=ls[3][:8]
            ARR=ls[-1].strip()
            ARR=ARR.replace(" ","0") # if minnute or second is lower than 10
            
            
            if (len(ARR)==9):
                self.arrtime="0"+ARR[:9] # if hour is lower than 10
            else:
                self.arrtime=ARR[:9] 

            
            #print "==",ARR[:9]
            #print ">>>", self.stacode, self.phase, datetime.datetime.strptime(self.arrtime, '%H%M%S.%f'),"<<<"
			
            if (DEBUG==1):
                print "READ:", self.stacode, self.phase, self.arrtime
            try:
                self.arrtime= datetime.datetime.strptime(self.arrtime, '%H%M%S.%f')
                self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
            except:
                return

        # -----------
        elif (self.auth == 'UCC'):
            ls=l.split()
            #print ls
            found_time=0
            self.stacode=ls[0].strip()
            self.dist=float(ls[2].strip())*km2deg
            print "UCC,",self.dist
                    
            self.phase="P"
            for i in range(1,len(ls)):
                #print ls[i], valid_date(ls[i]), found_time
                # Deux temps d'arrivée (P et S) sur une même ligne
                if (valid_date(ls[i])): 
                    if (found_time==0):
                        #print "=P",i,len(ls)
                        self.arrtime=ls[i]
                        self.phase="P"
                        found_time+=1
                        continue
                    if (found_time==1): 
                        #print "=S",i,len(ls)
                        self.Sarrtime=ls[i]
                        #self.phase="S"
                        found_time=0    

        # -----------
        elif (self.auth == 'GEN'):
            ls=l.split()

            found_time=0
            self.stacode=ls[0].strip()

            if (ls[1]=='--:--'):
                self.phase="Sg"
                self.arrtime=ls[7].strip()

            else:
                self.phase="Pg"
                self.arrtime=ls[3].strip()
                self.Sarrtime=ls[9].strip()                

        # -----------
        elif (self.auth in ['MAD','ICC']):
            self.stacode, self.dist, self.azim, self.phase, self.arrtime=l.split()[0].strip(),float(l.split()[1]),float(l.split()[2]),l.split()[3].strip(),l.split()[4]
            if (self.stacode=='EMING'): # Pas bien de coder en dur !  EMING est une stations LED mais LED publie EMIN que l'on transforme en EMING. Pour MAD, il faut revenir à EMIN.
                self.stacode='EMIN'
            if (DEBUG==1):
                print "MAD/ICC: ",self.stacode, self.dist, self.azim, self.phase, self.arrtime

        # -----------
        elif (self.auth == 'INGV'):
            ls=l.split()
            self.weight=float(ls[-1])/100
            if (self.weight>=0.5): # weight
                self.dist, self.azim=float(ls[7]),float(ls[6])
                self.stacode=ls[0].split(".")[1].strip()
                self.phase=ls[5]
                self.arrtime=ls[1]
                self.arrtime= datetime.datetime.strptime(self.arrtime, '%Y-%m-%dT%H:%M:%S.%f')
                self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
                

        # -----------
        elif (self.auth == 'LED'):
            l=l[5:]
            ls=l.split()
                      
            if (match(r"([A-Z])+", ls[0]) is None):
                if (len(mysta)>0):
                    self.dist, self.azim=float(ls[0]),float(ls[1])
                    self.stacode=mysta
                    #mysta=""
            else:
                self.stacode=ls[0].strip()
                mysta=self.stacode
                self.dist, self.azim=float(ls[1]),float(ls[2])
           
            self.dist=self.dist*km2deg
                   
            if (DEBUG==1):
                print "%s|%s|%s" % (l[25:27], l[28:30], l[31:36])

            self.weight=float(l[51:55])
            ##print "----",self.weight
            
            self.arrtime = "%2.2d:%2.2d:%04.1f" % (int(l[25:27].strip()), int(l[28:30].strip()), float(l[31:36].strip()))
            self.phase= l[40]
            if (DEBUG==1):
                print ">>",self.stacode, self.dist, self.azim, self.phase, self.arrtime

        # -----------
        elif (self.auth == 'KNMI'):
            ls=l.split("\t")
            self.dist = float(ls[2])*km2deg
            self.stacode= ls[0]
            self.phase= ls[3]
            self.arrtime = ls[4].strip()
            self.arrtime= datetime.datetime.strptime(self.arrtime, '%H:%M:%S.%f')
            self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
            if (DEBUG==1):
                print ">>",self.stacode, self.dist, self.azim, self.phase, self.arrtime 

        elif (self.auth == 'IMP'):
            l=l.strip()
            self.stacode= l[0:5].strip()
            self.phase= l[6:8].strip()
            self.arrtime=l[18:30].strip()
            self.arrtime= datetime.datetime.strptime(self.arrtime, '%H:%M:%S.%f')
            self.arrtime=self.arrtime.strftime("%H:%M:%S.%f")
                        
        # -----------
        else:
            print "Réseau %s inconnu" % self.auth
            return

        if (DEBUG==1):
            ##print l.split("\t")
            print "P & S arrival times: ", self.arrtime, self.Sarrtime
        
        epoch = datetime.datetime(1970,1,1)

        #print "-------------",self.arrtime
        picktime=datetime.datetime.strptime(self.arrtime, '%H:%M:%S.%f')
        picktime=picktime.replace(year=self.otime.year, month=self.otime.month, day=self.otime.day)            
            
        picktime_unix = (picktime - epoch).total_seconds()
                        
        # Cas d'un temps origine juste avant 00:00 avec des temps d'arrivée après 00:00, donc le jour d'après
        if (picktime_unix<self.otime_unix):
            picktime_unix+=86400

        self.dtime_unix=picktime_unix-self.otime_unix # delta entre origin et arrivée  
        self.arrtime=picktime_unix

        if (self.Sarrtime!=0):
            
            Spicktime=datetime.datetime.strptime(self.Sarrtime, '%H:%M:%S.%f')
            Spicktime=Spicktime.replace(year=self.otime.year, month=self.otime.month, day=self.otime.day)
                    
            Spicktime_unix = (Spicktime - epoch).total_seconds()
            #print Spicktime_unix
                            
            # Cas d'un temps origine juste avant 00:00 avec des temps d'arrivée après 00:00, donc le jour d'après
            if (Spicktime_unix<self.otime_unix):
                Spicktime_unix+=86400
                                   
            self.Sarrtime=Spicktime_unix
            #print self.Sarrtime
    
                
#------------------------------------
class AjoutArr():
    def __init__(self, orid, auth):
        self.ajout=0

        self.orid=orid
        self.auth=auth

        self.WORKDIR=os.path.dirname(__file__)
        self.ROOT=os.path.splitext(os.path.basename(__file__))[0]
        self.CFGFILE="%s/%s.cfg" % (self.WORKDIR,self.ROOT)

        # ATTENTION: pour Windows uniquement !!
        # Ici, on a besoin de créer un fichier txt. Pour éviter les écritures concurrentes, on ouvre un fichier en local
        self.AJfile="%s/%s.%s.txt" % (os.getenv('userprofile'),self.ROOT,self.auth)
        
        
    def read_cfg(self):
        if ( os.path.getsize(self.CFGFILE) == 0 ):
            exit("Fichier de config (%s) introuvable !" % self.CFGFILE)
        try:
            fCFG=open(self.CFGFILE,'r')
        except IOError:
            raise
        
        for l in fCFG:
            if (match("ENGINE", l.split("=")[0])):
                self.ENGINE=str(l.split("=")[1]).strip()
            if (match("COMMID", l.split("=")[0])):
                self.COMMID=int(l.split("=")[1])
            if (match("CHAN", l.split("=")[0])):
                self.CHAN=l.split("=")[1].strip()
            if (match("STYPE", l.split("=")[0])):
                self.STYPE=l.split("=")[1].strip()
            if (match("QUAL", l.split("=")[0])):
                self.QUAL=l.split("=")[1].strip()
            if (match("MAX_DTIME", l.split("=")[0])):
                self.MAX_DTIME=int(l.split("=")[1])
            if (match("MAX_DIST_PG", l.split("=")[0])):
                self.MAX_DIST_PG=float(l.split("=")[1])
            if (match("MAX_DIST_PN", l.split("=")[0])):
                self.MAX_DIST_PN=float(l.split("=")[1])

        fCFG.close()

    
    def load_auth_file(self, ):
        osCommandString = "notepad.exe %s" % self.AJfile
        os.system(osCommandString)

    def get_oritime(self):
        R=self.conn.execute("select time from origin where orid=%d" % self.orid)
        return R.fetchone()[0]

    def get_next_arid(self):
        R=self.conn.execute("select arrival_arid.nextval from dual")
        return R.fetchone()[0]
    
    def get_chanid(self,stacode):
        R=self.conn.execute("select NVL(max(chanid),0) from   sitechan where  sta  = '%s' and  chan = '%s'" % (stacode,self.CHAN))
        return R.fetchone()[0]

    def verif_format(self,arr):
        if (match(r"([A-Z])+", arr.stacode) is None):
            return 0
        if ((match(r"(^[PS][ng]$)", arr.phase) is None) & (match(r"^[PS]$", arr.phase) is None)):
            return 0
        return 1

    def verif_already_exists_sta_phase(self,arr):
        R=self.conn.execute("select count(*) from assoc where sta  = '%s' and  phase = '%s' and orid=%d" % (arr.stacode,arr.phase, self.orid))
        return R.fetchone()[0]

    def verif_already_exists_sta(self,arr):
        R=self.conn.execute("select count(*) from assoc where sta  = '%s' and orid=%d" % (arr.stacode, self.orid))
        return R.fetchone()[0]

    def convert_sta_code(self, arr):
        R1=self.conn.execute("select sta2 from Sismic.Ajastasta where sta1 = '%s'" % (arr.stacode))
        R2=self.conn.execute("select refsta from site where sta = '%s' and statype='cc'" % (arr.stacode))
        
        try:
            STASTA=R1.fetchone()[0]
            return STASTA
        except:  
            try:
                REFSTA=R2.fetchone()[0]
                return REFSTA
            except:
                return arr.stacode
                
    def verif_knows_station(self,arr):
        R=self.conn.execute("select count(*) from affiliation a, sitechan s \
                            where a.sta = '%s' and a.net='NEP'\
                            and a.sta=s.sta and s.groupname not in('NEP-CP','NEP-BB')" % (arr.stacode))
        return R.fetchone()[0]

    def verif_chan_is_CPZ(self,arr):
        R=self.conn.execute("select count(*) from sitechan where sta = '%s' and chan ='CPZ'" % (arr.stacode))
        return R.fetchone()[0]

    def assign_Pg_Pn(self,arr):
        print arr.phase, arr.dist
        if (arr.phase=='P'):
			if (arr.dist != 0):
				if (arr.dist <= self.MAX_DIST_PG):
					arr.phase='Pg'
				elif (arr.dist <= self.MAX_DIST_PN):
					arr.phase='Pn'
			else:
				arr.phase='Pg'
        if (arr.phase=='S'):
			if (arr.dist != 0):
				if (arr.dist <= self.MAX_DIST_PG):
					arr.phase='Sg'
				elif (arr.dist <= self.MAX_DIST_PN):
					arr.phase='Sn'
			else:
				arr.phase='Sg'
        
    # ------------------------------------
    def genere_sql_commande(self):
        for arr in self.ALLarr:
            insert_arrival="insert into arrival (sta, time ,arid,stassid,chanid,chan,iphase,stype,deltim,azimuth,delaz,slow,delslo,ema,rect,amp,per,logat,\
clip,fm,snr,qual,gain,duree,auth,commid,amptime,duration,ampmin,gain2)\
values ('%s', %.1f, %d,(-1),%d,'%s','%s','%s',0,(-180),0,0,0,(-180),0,0,0,0,null,null,0,'%s',0,0,'%s',%d,0,0,0,0)" \
% (arr.stacode, arr.arrtime, arr.arid, arr.chanid, arr.chan, arr.phase, arr.stype, arr.qual, self.auth, arr.commid)
            
            insert_assoc="insert into assoc (arid,orid,sta,phase,belief,delta,seaz,esaz,timeres, timedef,azres,azdef,slores,slodef,emares,wgt,vmodel,commid)\
values (%d,%d,'%s','%s',1,(-1),(-180),(-180),(-999),'d',(-999),'-',(-999),'-',(-999),(-1),'-',%d)" \
% (arr.arid, self.orid, arr.stacode, arr.phase, arr.commid)
            
            #print insert_assoc
            #print insert_arrival
          
            self.conn.execute(insert_assoc)
            self.conn.execute(insert_arrival)
            
    # ------------------------------------
    #def switch_P_and_S():
    
    # ------------------------------------
    def parse_bull(self):

        self.conn=connect_dtb(self.ENGINE)

        fbull=open(self.AJfile,'rb')

        self.otime_unix=float(self.get_oritime())
        self.otime=TIMESTAMP_TO_DATETIME(self.otime_unix)

        self.ALLarr=[]

        prevSta=''
        prevPhase=''
        
        global isP
        isP=1 #first phase is P (isP=1) and second is S (isP=0) by default

        global mysta
        mysta=""


        AJOUTARR_ECH_DIR="K:/echanges/cenalt/AjoutArr"
        #uk_sta_output_file="%s/ajoutarr_unknown_stations.txt" % (os.getenv('userprofile'))
        uk_sta_output_file="%s/ajoutarr_unknown_stations.%s.txt" % (AJOUTARR_ECH_DIR,os.getenv('username'))
        fusta=open(uk_sta_output_file,'ab')
        now=datetime.datetime.now()
                        
        for line in fbull:
                A=Arrival(self.conn, self.otime, self.otime_unix, self.auth)
                ###A.load_bull_line(line)  ##########################################################################################
                ###continue  				##########################################################################################
                try:
                    A.load_bull_line(line)
                except:
                    print "(INFO) Pointé exclu: %s" % line,
                    continue

                A.chanid=self.get_chanid(A.stacode)
                A.chan=self.CHAN
                A.commid=self.COMMID
                A.stype=self.STYPE
                A.qual=self.QUAL

                if (A.weight<0.1):
                    print "Weight is too low (%.1f). Discard this arrival" % A.weight
                    continue
               
                # On vérifie si la P arrive bien avant la S. Si non, on intervertit les 2 temps d'arrivée
                if ( A.stacode != prevSta):
                    if (DEBUG==1):
                        print "DEBUG 1"
                    prevA=A
                    prevSta=A.stacode
                    prevPhase=A.phase
                else: # Si le temps de la S est inférieur au temps de la P, on inverse les temps d'arrivée
                    if (DEBUG==1):
                        print "DEBUG 2"
                    print "Check current and previous arrival times for station %s" % (A.stacode)
                    if ((A.phase[0]=='S') & (prevPhase[0]=='P')):
                        if (A.arrtime<prevA.arrtime):
                            print "WARN: %s if before %s for station %s !" % (A.phase, prevPhase, A.stacode)
                            print "WARN: Switch these arrival times"
                            Ptime=A.arrtime
                            Stime=prevA.arrtime
                            A.arrtime=Stime
                            prevA.arrtime=Ptime
                            
                    if ((A.phase[0]=='P') & (prevPhase[0]=='S')):
                        if (A.arrtime>prevA.arrtime):
                            print "WARN: %s if after %s for station %s !" % (A.phase, prevPhase, A.stacode)
                            print "WARN: Switch these arrival times"
                            Ptime=prevA.arrtime
                            Stime=A.arrtime
                            A.arrtime=Stime
                            prevA.arrtime=Ptime
                            
                    prevA=A
                    prevSta=A.stacode
                    prevPhase=A.phase

                # Convertit les codes de stations déjà utilisés par une autre station mondiale en un code unique, utilisable par ONYX
                A.stacode=self.convert_sta_code(A)

                #print A.stacode, A.phase, A.arrtime, "weight:",A.weight
                
                                        
                # On vérifie le bon format des stations et des phases
                # On ne prend que les nouveaux temps d'arrivées
                # On n'autorise que les temps d'arrivées entre T0 et T0+MAX_DTIME
                # On vérifie que la station est connue par Onyx
                # On modifie les phases P/S en Pg/Sg ou Pn/Sn en fonction de la distance
                    
                try:
                    self.assign_Pg_Pn(A)
                    if (DEBUG==1):
                        print "DEBUG 4",A.dtime_unix, self.MAX_DTIME
                    if (self.verif_knows_station(A) == 0):
                        print '(WARN) Unknown station: %s' % A.stacode
                        fusta.write("%s %s %s\n" % (now,A.auth,A.stacode))
                        
                    print "Format(A):", self.verif_format(A),"(1); Already exists:", self.verif_already_exists_sta(A), "(0); Dtime:",(A.dtime_unix<self.MAX_DTIME), "(True); known station:", A.stacode, self.verif_knows_station(A), "(1); chan CPZ:", self.verif_chan_is_CPZ(A), "(1)"
                    if ((self.verif_format(A)) & (self.verif_already_exists_sta(A)==0) & (A.dtime_unix<self.MAX_DTIME) & (self.verif_knows_station(A)>=1) & (self.verif_chan_is_CPZ(A)>=1)):
                        print "DEBUG 5"
                        A.arid=self.get_next_arid()
                        self.ALLarr.append(A)                                              
                except:
                    pass
                
                if (A.Sarrtime!=0):
                    B=Arrival(self.conn, self.otime, self.otime_unix, self.auth)
                    B.stacode=A.stacode
                    B.chanid=A.chanid
                    B.chan=self.CHAN
                    B.commid=self.COMMID
                    B.stype=self.STYPE
                    B.qual=self.QUAL
                    B.phase="S"
                    B.dist=A.dist
                    B.arrtime=A.Sarrtime

                    if (DEBUG==1):
                        print "DEBUG 3",B.stacode, B.phase, B.arrtime

                try:                    
                    if (A.Sarrtime!=0): # Si un temps d'arrivée de S est présent en plus du temps de la P
                        self.assign_Pg_Pn(B)
                        print "Format(B):", self.verif_format(B),"(1); Already exists:", self.verif_already_exists_sta(B), "(0); Dtime:",(A.dtime_unix<self.MAX_DTIME), "(True); known station:", B.stacode, self.verif_knows_station(B), "(1); chan CPZ:", self.verif_chan_is_CPZ(B), "(1)"
                        # Attention, B.dtime_unix n'existe pas. On prend A.dtime_unix
                        if ((self.verif_format(B)) & (self.verif_already_exists_sta(B)==0) & (A.dtime_unix<self.MAX_DTIME) & (self.verif_knows_station(B)>=1) & (self.verif_chan_is_CPZ(B)>=1)):
                            print "DEBUG 6"
                            B.arid=self.get_next_arid()
                            self.ALLarr.append(B)
                except:
                    pass
					
                print

        
        fbull.close()

        fusta.close()
        
        return self
    
