#-*- coding: iso-8859-15 -*-

import Tkinter as tk
import ttk

myfont=("Arial", 10, "bold")

from  ajoutarr_tools import *

class Mytk(tk.Frame):
    def __init__(self, master, AJ):
        master.geometry('450x400')
        self.master = master
        tk.Frame.__init__(self, master)

        self.btn = tk.Button(master, text='Ajouter ces temps', command=self.addtime_and_destroy)
        self.btn.pack(fill='y')

        self.tree = ttk.Treeview(self.master, height=15, columns=('Num','Station','Phase','Time'))

        self.tree.heading('#0', text='#')
        self.tree.heading('#1', text='Station')
        self.tree.heading('#2', text='Phase')
        self.tree.heading('#3', text='Time')
        self.tree.column('#0', stretch=tk.YES, minwidth=30, width=50, anchor='n')
        self.tree.column('#1', stretch=tk.YES, minwidth=10, width=100, anchor='n')
        self.tree.column('#2', stretch=tk.YES, minwidth=10, width=100, anchor='n')
        self.tree.column('#3', stretch=tk.YES, minwidth=50, width=150, anchor='n')
        #self.tree.grid(row=4, columnspan=10, sticky='nsew')
        self.treeview=self.tree
        self.tree.pack(expand=True, fill='both')

        self.exit_button = tk.Button(master, text='Annuler', command=self.just_destroy)
        self.exit_button.pack(fill='x')
        
        self.rclick = RightClick(self.master,AJ)
        self.num = 0

        # attach popup to treeview widget
        self.tree.bind('<Button-3>', self.rclick.popup)
        

    def clickbtn(self):
        text = '#' + str(self.num)
        self.tree.insert('', 'end', text=text)
        self.num += 1

    def addtime_and_destroy(self):
        global AJOUTE
        AJOUTE=1
        self.master.destroy()
        
    def just_destroy(self):
        global AJOUTE
        AJOUTE=0
        self.master.destroy()

class RightClick:
    def __init__(self, master, AJ):
        # create a popup menu
        self.aMenu = tk.Menu(master, tearoff=0)
        self.aMenu.add_command(label='Delete', command=self.delete)
        self.tree_item = ''
        
    def delete(self):
        global app
        if self.tree_item:
            curItem = app.tree.focus()
            SPT2delete=app.tree.item(curItem)['values']

            for arr in AJ.ALLarr:
                if ((arr.stacode == SPT2delete[0]) & (arr.phase == SPT2delete[1])):
                    if (arr in AJ.ALLarr):
                        print "delete",arr.stacode, arr.phase
                        AJ.ALLarr.remove(arr)
            
            app.tree.delete(self.tree_item)
        #print len(AJ.ALLarr)
            
    def hello(self):
        print ('hello!')

    def popup(self, event):
        global app
        self.aMenu.post(event.x_root, event.y_root)
        self.tree_item = app.tree.focus()

    def selectItem(self,a):
        global app
        curItem = app.tree.focus()
        print app.tree.item(curItem)


# ------------------------------------
def show_parsed_bulletin(aj):

        if (len(aj.ALLarr)==0):
            print "Aucun temps d'arrivée à ajouter."
            exit()
            
        global app
        global AJOUTE
        AJOUTE=0
        
        root = tk.Tk()
        app=Mytk(root,aj)
            
        inum=1
        for arr in aj.ALLarr:
            print arr.stacode,arr.phase,str(TIMESTAMP_TO_DATETIME(arr.arrtime))[:21]
            app.treeview.insert('', 'end', text=inum, values=(arr.stacode,arr.phase,str(TIMESTAMP_TO_DATETIME(arr.arrtime))[:21]))
            inum+=1

        root.mainloop()

# ------------------------------------
class Select_Agency(tk.Frame):
    def __init__(self, master):
        master.geometry('600x600')
        self.master = master
        tk.Frame.__init__(self, master)

        style = ttk.Style()
        style.configure(".", font=('Arial', 12), foreground="white")
        style.configure("Treeview", foreground='black',rowheight=30)
        style.configure("Treeview.Heading", foreground='black', font=('Arial', 12))

        self.tree = ttk.Treeview(self.master, columns=('Institut','Pays'))
        self.tree.heading('#0', text='Institut', anchor='n')
        self.tree.heading('#1', text='Pays, région', anchor='n')
        self.tree.column('#0', stretch=tk.YES, minwidth=30, width=150, anchor='w')
        self.tree.column('#1', stretch=tk.YES, minwidth=50, width=300, anchor='w')
        self.treeview=self.tree
        self.tree.pack(expand=True, fill='both')

        self.tree.bind('<Double-Button-1>', self.OnDoubleClick)
        
        self.btn = tk.Button(master, text='Sélectionner cet institut', font=myfont, command=self.select_and_destroy, pady=16)
        
        #self.btn.pack(expand='True', fill='x')
        self.btn.pack(side="left")
        self.exit_button = tk.Button(master, bg='grey', text='              Annuler              ', font=myfont, command=self.just_destroy, pady=16)
        self.exit_button.pack(expand='True', side='right')
        #self.btn.pack(expand='True',side="right")

    def clickbtn(self):
        text = '#' + str(self.num)
        self.tree.insert('', 'end', text=text)
        self.num += 1

    def select_and_destroy(self):
        global agency_app
        global myauth
        global SELECT

        SELECT=1
        curItem = agency_app.tree.focus()
        myauth = agency_app.tree.item(curItem)['text']
        print ("you clicked on", agency_app.tree.item(curItem)['text'])
        self.master.destroy()

    def OnDoubleClick(self, app):
        global agency_app
        global myauth
        global SELECT

        SELECT=1
        curItem = agency_app.tree.focus()
        myauth = agency_app.tree.item(curItem)['text']
        self.master.destroy()
        
    def just_destroy(self):
        global SELECT
        SELECT=0
        self.master.destroy()


# ------------------------------------      
def choose_agency(agency_dict):
        global agency_app

        AGENCIES=agency_dict.keys()

        root = tk.Tk()
        agency_app=Select_Agency(root)
        
        inum=1
        for agency in AGENCIES:
            if (inum % 2 != 0):
                agency_app.treeview.insert('', 'end', text=agency, values=(agency_dict[agency]), tags = ('oddrow',))
            else:
                agency_app.treeview.insert('', 'end', text=agency, values=(agency_dict[agency]), tags = ('evenrow',))
            inum+=1
  
        agency_app.treeview.tag_configure('oddrow', background='grey', foreground='black', font=myfont)
        agency_app.treeview.tag_configure('evenrow', background='lightgrey', foreground='black', font=myfont)

        root.mainloop()
        
#-----------------------------------------------------
if __name__ == "__main__":
    
    WORKDIR=os.path.dirname(__file__)
    print "WORKDIR=", WORKDIR

    global DEBUG
    if (search("\DEV", WORKDIR)):
        DEBUG=1
    else:
        DEBUG=0
            
    AUTHORIZED_AGENCIES_FILE="%s/authorized_agencies.txt" % (WORKDIR)

    if ( os.path.getsize(AUTHORIZED_AGENCIES_FILE) == 0 ):
        exit("Fichier de config (%s) introuvable !" % AUTHORIZED_AGENCIES_FILE)
    try:
        fAG=open(AUTHORIZED_AGENCIES_FILE,'r')
    except IOError:
        raise

    from collections import OrderedDict
    LIST_OF_AGENCIES=OrderedDict()
    for l in fAG:
        LIST_OF_AGENCIES[l.split(':')[0].strip().decode('utf-8')]=l.split(':')[1].strip().decode('utf-8')
    fAG.close()
  
    global myauth
    global SELECT
    SELECT=0
  
    if (len(argv)==3):
        myorid=int(argv[1])
        myauth=argv[2]
        SELECT=1
    elif (len(argv)==2):
        myorid=int(argv[1])
        choose_agency(LIST_OF_AGENCIES)
    else:
        ########## DEBUG ###########
        if (DEBUG==1):

            myauth='INGV'
            myorid=5005081
            myauth='INGV'
            myorid=5006315

            myauth='GFZ'
            myorid=5006673

            myauth='KNMI'
            myorid=5006564

            myauth='BGS'
            myorid=5009275
            
            choose_agency(LIST_OF_AGENCIES)
        ########## DEBUG ###########
        else:
            print "Usage: python ajoutarr.py <ORID>"
            print "Usage: python ajoutarr.py <ORID> <AGENCE>"
            exit()

    if (SELECT==0):
        exit()

    if (myauth not in LIST_OF_AGENCIES.keys()):
        print("L'agence %s est inconnue" % myauth)
        exit()
            
    if (myauth=='OCA'):
        myauth='OCA_M'

    AJ=AjoutArr(myorid,myauth)
    AJ.read_cfg()
    AJ.load_auth_file()
    AJ.parse_bull()

    show_parsed_bulletin(AJ)
    
    if (AJOUTE == 1):
        AJ.genere_sql_commande()
    else:
        print "Ok, on ne change rien."
    
    close_dtb(AJ.conn)    

    

