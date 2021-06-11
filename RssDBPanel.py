'''rssDB GUI interface
rssDB is a tool to collect and store SEC XBRL feeds.
'''

import os, sys, time, json, gettext, re, datetime, threading, traceback, logging, queue
from collections import defaultdict
from dateutil import tz
from datetime import timedelta
from lxml import html, etree
from urllib import request
from arelle import ViewWinRssFeed, ModelDocument, ViewWinProperties, FileSource
from arelle.FileSource import openFileSource
from arelle.ViewWinList import ViewList
from arelle.Locale import format_string
from arelle.ModelXbrl import ModelXbrl, load as mXLoad
from arellepy.HelperFuncs import chkToList
from arelle.CntlrWinTooltip import ToolTip
from arelle.UiUtil import checkbox, gridCombobox, label, gridCell
from arelle.ViewWinTree import ViewTree
from arellepy.CntlrPy import CntlrPy, runFormulaFromDBonRssItems, makeFormulaDict
from arellepy.HelperFuncs import getExtractedXbrlInstance
# from arelle.DialogUserPassword import askDatabase
try:
    from .RssDB import rssDBConnection 
    from .Constants import DBTypes, pathToResources
    from .CommonFunctions import _makeRssFeedLikeXml, storeInToXbrlDB, _dbTypes, dbProduct
except:
    from rssDB.RssDB import rssDBConnection 
    from rssDB.Constants import DBTypes, pathToResources
    from rssDB.CommonFunctions import _makeRssFeedLikeXml, storeInToXbrlDB, _dbTypes, dbProduct

import tkinter as tkr
from tkinter import messagebox, simpledialog
from tkinter import filedialog
try:
    import tkinter.ttk as ttk
except ImportError:
    import ttk

# store UIs dependent on connection to handle when disconnecting while those UIs are still open.
con_dependent_ui = []

currDir = os.path.dirname(os.path.abspath(__file__))
# testing stuff
setConfigDir = None
targetResDir = None
getQueue_render = False
getQueue_update = False


MAKEDOTS_RSSDBPANEL = dict()

def dotted(cntlr, _key, xtext='Processing'):
    '''Just let me know you are alive!'''
    global MAKEDOTS_RSSDBPANEL
    n = 1
    while MAKEDOTS_RSSDBPANEL.get(_key, False):
        _xtext = xtext + '.'*n
        cntlr.waitForUiThreadQueue()
        cntlr.uiThreadQueue.put((cntlr.showStatus,[_xtext]))
        if n >=15:
            n = 1
        else:
            n+=1
        time.sleep(.8)
    cntlr.waitForUiThreadQueue()
    cntlr.uiThreadQueue.put((cntlr.showStatus,['']))
    return

with open(os.path.join(pathToResources,'industryTree.json'), 'r') as industries:
    res = json.load(industries)

def flatenIndustry(a: dict, res=dict()):
    for k,v in a.items():
        res[k] = v['description']
        if 'children' in v.keys():
            flatenIndustry(v['children'], res)
    return res

flatIndustry = flatenIndustry(res)

industryCodesSelection = tuple()

DBDescriptions = ("Postgres", "SQLite", "MongoDB")

searchResults = 0


def makeWeight(frame, columns=True, rows=True, _weight=1):
    cols, _rows = frame.grid_size()
    if columns:
        for c in range(0, cols):
            frame.columnconfigure(c, weight=_weight)
    if rows:
        for r in range(0, _rows):
            frame.rowconfigure(r, weight=_weight)
    return

def toolBarExtender(cntlr, toolbar): 
    addTosysPath = cntlr.config.setdefault('rssDBaddToSysPath', [])
    if addTosysPath:
        for p in addTosysPath:
            sys.path.append(p)
    def openRssDBPanel(btn, cntlr=cntlr):
        btn.config(state='disabled')
        topL = tkr.Toplevel(cntlr.parent)
        topL.title('SEC XBRL RSS DB')

        frame_canvas = tkr.Frame(topL)
        frame_canvas.grid(row=0, column=0, sticky=tkr.NSEW)
        f = rssDBFrame(frame_canvas,cntlr=cntlr) #canvas
        f.grid(row=0, column=0, sticky=tkr.NSEW, padx=3, pady=3)
        makeWeight(frame_canvas)
        makeWeight(topL)

        def closeAction():
            confirm = True
            if f.dbConnection:
                if f.dbConnection.checkConnection():
                    confirm = tkr.messagebox.askyesno(title=_('Exit RSS DB Connection'),
                                                      message=_('Closing this window will disconnect current connection\n\nDo you want to proceed?'), icon='warning')
            if confirm:
                chk = f.disconnectDB(1)
                if chk:
                    cntlr.parent.focus_set()
                    btn.config(state='normal')
                    topL.destroy()
                else:
                    return
            else:
                return
        topL.protocol("WM_DELETE_WINDOW", closeAction)

    if cntlr.isMac:
        toolbarButtonPadding = 1
    else:
        toolbarButtonPadding = 4

    tbControl = ttk.Separator(toolbar, orient=tkr.VERTICAL)
    column, row = toolbar.grid_size()
    tbControl.grid(row=0, column=column, padx=6)
    image = os.path.join(currDir, 'resources/rssIcon.png')
    image = tkr.PhotoImage(file=image)
    cntlr.toolbar_images.append(image)
    tbControl = ttk.Button(toolbar, image=image, command= lambda: openRssDBPanel(tbControl, cntlr), style="Toolbutton", padding=toolbarButtonPadding)
    ToolTip(tbControl, _('Launch rssDB panel'))
    column, row = toolbar.grid_size()
    tbControl.grid(row=0, column=column)
    return

def rssDB_showLoadedXbrl(cntlr, modelXbrl, attach, selectTopView=False, queryParams=None, q=None):
    global searchResults
    conParams = cntlr.dbConnection.conParams
    conName = '{}{} - {}'.format(conParams.get('host', False) + '/' if conParams.get('host', False) else '', 
                                    os.path.basename(conParams.get('database', '')) if conParams.get('database', '') else '', 
                                    conParams.get('product', ''))
    startedAt = time.time()
    currentAction = "setting title"
    topView = None
    cntlr.currentView = None
    try:
        if attach:
            modelXbrl.closeViews()
        cntlr.parent.title(_("arelle - {0}").format(
                        os.path.basename(modelXbrl.modelDocument.uri)))
        cntlr.setValidateTooltipText()
        if modelXbrl.modelDocument.type == ModelDocument.Type.RSSFEED:
            currentAction = "view of RSS feed"
            # ViewWinRssFeed.viewRssFeed(modelXbrl, cntlr.tabWinTopRt)
            searchResults +=1
            rssDBviewRssFeed(modelXbrl, cntlr.tabWinTopRt, 'Search Result #{} {}'.format(str(searchResults), conName), queryParams, q)
            
            topView = modelXbrl.views[-1]
        else:
            pass
        currentAction = "property grid"
        ViewWinProperties.viewProperties(modelXbrl, cntlr.tabWinTopLeft)
        currentAction = "log view creation time"
        viewTime = time.time() - startedAt
        modelXbrl.profileStat("view", viewTime)
        cntlr.addToLog(format_string(cntlr.modelManager.locale, 
                                    _("views %.2f secs"), viewTime))
        if selectTopView and topView:
            topView.select()
        cntlr.currentView = topView
        currentAction = "plugin method CntlrWinMain.Xbrl.Loaded"
    except Exception as err:
        msg = _("Exception preparing {0}: {1}, at {2}").format(
                    currentAction,
                    err,
                    traceback.format_tb(sys.exc_info()[2]))
        tkr.messagebox.showwarning(_("Exception preparing view"),msg, parent=cntlr.parent)
        cntlr.addToLog(msg)
    cntlr.showStatus(_("Ready..."), 2000)
    return
    
def rssDB_backgroundLoadXbrl(cntlr, filesource, importToDTS, selectTopView, queryParams=None, q=None):
    '''Based on CntlrWinMain.backgroundLoadXbrl'''
    startedAt = time.time()
    if cntlr.dbConnection.searchResultsModelXbrl:
        # use existing view
        cntlr.dbConnection.searchResultsModelXbrl.reload('Updating View', True)
    else:
        try:
            action = _("loaded")
            profileStat = "load"
            modelXbrl = cntlr.modelManager.load(filesource, _("views loading"), checkModifiedTime=False) # check modified time if GUI-loading from web
            cntlr.dbConnection.searchResultsModelXbrl = modelXbrl
        except ModelDocument.LoadingException:
            cntlr.showStatus(_("Loading terminated, unrecoverable error"), 15000)
            return
        except Exception as err:
            msg = _("Exception loading {0}: {1}, at {2}").format(
                        filesource.url,
                        err,
                        traceback.format_tb(sys.exc_info()[2]))
            cntlr.addToLog(msg)
            cntlr.showStatus(_("Loading terminated, unrecoverable error"), 15000)
            return
        if modelXbrl and modelXbrl.modelDocument:
            statTime = time.time() - startedAt
            modelXbrl.profileStat(profileStat, statTime)
            cntlr.addToLog(format_string(cntlr.modelManager.locale, 
                                        _("%s in %.2f secs"), 
                                        (action, statTime)))
            cntlr.showStatus(_("{0}, preparing views").format(action))
            cntlr.waitForUiThreadQueue() # force status update
            cntlr.uiThreadQueue.put((rssDB_showLoadedXbrl, [cntlr, modelXbrl, importToDTS, selectTopView, queryParams, q]))
        else:
            cntlr.addToLog(format_string(cntlr.modelManager.locale, 
                                        _("not successfully %s in %.2f secs"), 
                                        (action, time.time() - startedAt)))
            cntlr.showStatus(_("Loading terminated"), 15000)
    return

class rssDBFrame(tkr.Frame):
    def __init__(self, master, cntlr=None, allInOne=False, **kw):
        super().__init__(master, **kw)
        if not cntlr:
            cntlr = CntlrPy(instConfigDir=setConfigDir, useResDir=targetResDir, logFileName="logToBuffer")
            _modelXbrl = ModelXbrl(cntlr.modelManager)
        gettext.install('Arelle')
        self.allInOne = allInOne
        self.runGetStat = True
        self.cntlr = cntlr
        self.getQueue = True
        self.multiprocessQueue = None #queue.Queue()
        self.parent = master
        self.dbConnection = None
        self.priorDatabaseSettingsList = cntlr.config.setdefault('rssDBconnection', [])
        self.currentSettingSelectionIndex = len(self.priorDatabaseSettingsList)-1 if len(self.priorDatabaseSettingsList) else 0

        self.connectionFrame = tkr.LabelFrame(self, text = _('Database Connection'), padx=5, pady=5)
        
        self.statFrame = tkr.Frame(self.connectionFrame)
        self.connectionIndicatorCanvas = tkr.Canvas(self.statFrame, width=12, height=12)
        self.connectionIndicatorCanvas.grid(row=0, column=0, sticky=tkr.W, padx=2, pady=2)
        self.connectionIndicator = self.connectionIndicatorCanvas.create_oval(2,2,10,10, fill='red')
        self.connectionIndicatorText = tkr.Label(self.statFrame, text=_('Not Connected to DB'), wraplength=350, justify="left")
        self.connectionIndicatorText.grid(row=0, column=1, columnspan=6, sticky=tkr.W)
        self.statFrame.grid(row=0, column=0, columnspan=7, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=2, pady=2)


        self.btn_connectToDb = tkr.Button(self.connectionFrame, text="Connect To RSS DB", command=self.connectToDB,) # width=13
        self.btn_disconnectDB = tkr.Button(self.connectionFrame,text="Disconnect RSS DB", command=lambda: self.disconnectDB(confirm=True),) #width=13
        ToolTip(self.btn_disconnectDB, _('Disconnet Current Connection'))
        self.btn_disconnectDB.config(state='disabled')
        self.btn_checkDbStat = tkr.Button(self.connectionFrame, text="Get DB Stat", command= lambda: self.backgroundGetDbStats(self.dbConnection),) #width=8
        ToolTip(self.btn_checkDbStat, _('Display DB stats in Arelle message panel'))
        self.btn_checkDbStat.config(state='disabled')        
        self.btn_showReport = tkr.Button(self.connectionFrame, text="DB Report", command= self.startDash, ) #width=8
        ToolTip(self.btn_showReport, _('Runs a web app the shows information about the submissions in the database, requires pandas, plotly and other packages '
                                        'you will be prompted for the path where these packages are installed (probably a python virtual environment)'), wraplength=360)
        self.btn_showReport.config(state='disabled')        

        self.btn_connectToDb.grid(row=1, column=0, columnspan=3, sticky=tkr.EW, pady=1, padx=1)
        self.btn_disconnectDB.grid(row=1, column=3, columnspan=2, sticky=tkr.EW, pady=1, padx=1)
        self.btn_checkDbStat.grid(row=1, column=5, columnspan=1, sticky=tkr.EW, pady=1, padx=1)  
        self.btn_showReport.grid(row=1, column=6, columnspan=1, sticky=tkr.EW, pady=1, padx=1)  

        r = 0
        self.connectionFrame.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5)

        if allInOne:
            r+=1
            self.updateDBFrame = rssDBUpdateSettings(self, text=_('Update RSS DB'), padx=5, pady=5)
            self.updateDBFrame.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5) 
            self.updateDBFrame.rssDBFrame = self.master
            self.updateDBFrame.update_btn.config(state='disabled')

            r+=1
            self.queryFrame = rssDBSearchDBPanel(self, text=_('Search RSS DB'), padx=5, pady=5)
            self.queryFrame.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5)
            self.queryFrame.rssDBFrame = self
            self.queryFrame.searchDB_btn.config(state='disabled')

            # r+=1
            # self.TestQueryFrame = tkr.LabelFrame(self, text=_('Search DB'), padx=5,pady=5)
            # self.btn_industrySelect = tkr.Button(self.TestQueryFrame,text=_("Select Industry"), command=self.openIndustrySelector)
            # self.btn_showIndustrySelection = tkr.Button(self.TestQueryFrame,text=_("Selection"), command=self.showSelections)
            # self.btn_TestSearch = tkr.Button(self.TestQueryFrame, text=_("Test Search"), command= self.testSearch)
            
            # self.btn_industrySelect.grid(row=0, column=0, sticky=tkr.W, pady=1, padx=1)
            # self.btn_showIndustrySelection.grid(row=0, column=1, sticky=tkr.W, pady=1, padx=1)
            # self.btn_TestSearch.grid(row=0, column=2, sticky=tkr.W, pady=1, padx=1)

            # self.TestQueryFrame.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5)
        else:
            r+=1
            self.btn_updateDB = tkr.Button(self, text=_('Update Database (opens another window)'), command=self.btn_cmd_updateDB)
            self.btn_updateDB.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5)
            ToolTip(self.btn_updateDB, text=_("Update SEC XBRL rss Feed database with specified options, opens in a new window"), wraplength=360)
            self.btn_updateDB.config(state='disabled')

            r+=1
            self.btn_searchDB = tkr.Button(self, text=_('Search DB (opens another window)'), command=self.btn_cmd_searchDB)
            self.btn_searchDB.grid(row=r, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=5, pady=5)
            ToolTip(self.btn_searchDB, text=_("Search SEC XBRL rss Feed database with specified options, opens in a new window"), wraplength=360)
            self.btn_searchDB.config(state='disabled')  

        makeWeight(self)
        makeWeight(self.connectionFrame)
        # self.bind("<Destroy>", self._destroy)
        
    def btn_cmd_updateDB(self):
        global con_dependent_ui
        self.btn_updateDB.config(state='disabled')
        topL = tkr.Toplevel(self)
        topL.title(_('UPDATE SEC XBRL RSS DB'))
        self.updateDBFrame = rssDBUpdateSettings(topL, text=_('Update RSS DB'), padx=5, pady=5)
        self.updateDBFrame.grid(row=0, column=0, sticky=tkr.NSEW, padx=5, pady=5)
        self.updateDBFrame.update_btn.config(state='normal')
        topL.rssDBFrame = self
        self.updateDBFrame.rssDBFrame = self
        con_dependent_ui.append(topL)

        def closeAction():
            global con_dependent_ui
            if getattr(self.updateDBFrame, 'setAutoUpdate', False):
                confirm = messagebox.askyesno(_('Confirm Closing Update Settings'),  _('Closing update settings will stop currently active DB auto-update.\n\nDo you want to proceed?'), parent=self)
                if confirm:
                    self.stopAutoUpdate()
                else:
                    return
            delattr(topL.rssDBFrame, 'updateDBFrame')
            con_dependent_ui.remove(topL)
            self.btn_updateDB.config(state='normal')
            topL.destroy()

        topL.close = closeAction
        topL.protocol("WM_DELETE_WINDOW", closeAction)
        makeWeight(topL)
    
    def btn_cmd_searchDB(self):
        global con_dependent_ui
        self.btn_searchDB.config(state='disabled')
        topL = tkr.Toplevel(self)
        topL.title(_('SEARCH SEC XBRL RSS DB'))
        self.queryFrame = rssDBSearchDBPanel(topL, text=_('Search RSS DB'), padx=5, pady=5)
        self.queryFrame.grid(row=0, column=0, sticky=tkr.NSEW, padx=5, pady=5)
        self.queryFrame.searchDB_btn.config(state='normal')
        topL.rssDBFrame = self
        self.queryFrame.rssDBFrame = self
        con_dependent_ui.append(topL)

        def closeAction():
            global con_dependent_ui
            delattr(topL.rssDBFrame, 'queryFrame')
            con_dependent_ui.remove(topL)
            self.btn_searchDB.config(state='normal')
            topL.destroy()
            return

        topL.close = closeAction
        topL.protocol("WM_DELETE_WINDOW", closeAction)
        makeWeight(topL)
        return

    def startDash(self):
        dbReport = self.dbConnection.startDBReport()
        if dbReport:
            msg = "DB Report started at {}".format(dbReport[1])
            messagebox.showinfo(title="RSS DB info", message=msg, parent=self.cntlr.parent)
            self.cntlr.addToLog(msg, messageCode="RssDB.Info", file="",  level=logging.INFO)
        # except Exception as e:
        #     messagebox.showerror(title="RSS DB error", message=traceback.format_exc(), parent=self.cntlr.parent)
        #     self.cntlr.addToLog(traceback.format_exc(), messageCode="RssDB.Error", file="",  level=logging.ERROR)
        return

    def stopAutoUpdate(self):
        if getattr(self, 'backgroundUpdateConn', False):
            con = self.backgroundUpdateConn
            if con.autoUpdateSet:
                con.autoUpdateSet = False
                if con.updateStarted:
                    self.cntlr.addToLog(_('Auto-updated will be stopped after current update'), messageCode="RssDB.Info", file=self.dbConnection.conParams.get('databae', ""), level=logging.INFO)
        else:
            self.cntlr.addToLog(_('No connection is set to auto-update currently'), messageCode="RssDB.Info", file=self.dbConnection.conParams.get('databae', ""), level=logging.INFO)
        
        return

    def _updateDB(self, cntrl, connParams: dict, doAllParams: dict):
        global getQueue_update
        self.updateDBFrame.update_btn.config(state='disabled')
        color = self.updateDBFrame.btn_stopAutoUpdate.cget("background")
        text = 'Updating Now'
        if self.updateDBFrame.setAutoUpdate:
            self.updateDBFrame.btn_stopAutoUpdate.config(state='normal', bg='orange', fg='white')
            text = 'Auto-Update ON'
        try:
            self.backgroundUpdateConn = rssDBConnection(cntrl, **connParams, createDB=False)
            self.updateDBFrame.update_btn.config(text=text)
            self.backgroundUpdateConn.doAll(**doAllParams)
        except Exception as e:
            getQueue_update = False
            tkr.messagebox.showerror(title='RSS DB Error - updateDB', message=str(e) + '\n' + traceback.format_exc())
        self.backgroundUpdateConn.close()
        self.backgroundUpdateConn = None
        if hasattr(self, 'updateDBFrame'): # might happen after window is closed
            self.updateDBFrame.setAutoUpdate = False
            self.updateDBFrame.update_btn.config(text="Update DB")
            self.updateDBFrame.update_btn.config(state='normal')
            self.updateDBFrame.btn_stopAutoUpdate.config(state='disabled',  bg=color, fg='black')
        getQueue_update = False
        return

    def _backgroundGetQ(self):
        global getQueue_update
        cntlr = self.cntlr
        callback = {'showStatus': cntlr.showStatus, 'addToLog': cntlr.addToLog}
        while getQueue_update:
            if not self.multiprocessQueue.empty():
                callbackName, args = self.multiprocessQueue.get()
                cntlr.waitForUiThreadQueue()
                cntlr.uiThreadQueue.put((callback[callbackName],args))
        return

    def backGroundUpdateDB(self, doAllArgs: dict = None):
        global getQueue_update
        connArgs = {k:v for k, v in self.dbConnection.conParams.items() if not k == 'cntlr'}
        doAllArgs['q'] = self.multiprocessQueue
        getQueue_update = True
        self.t1 = threading.Thread(target=self._updateDB, args=(self.cntlr, connArgs, doAllArgs), daemon=True)
        # self.t2 = threading.Thread(target=self._backgroundGetQ, daemon=True)
        # self.t2.start()
        self.t1.start()
        return
  
    def backgroundSearchDB(self, params:dict):
        conn = self.dbConnection
        if not conn or not getattr(conn, 'conParams', False):
            self.cntlr.addToLog(_('No SEC RSS DB Connection'), messageCode="RssDB.Info", file="",  level=logging.INFO)
            self.cntlr.addToLog('')
            self.cntlr.logView.listBox.see(tkr.END)
            return
        if conn.product == 'sqlite':
            _conParams = conn.conParams
            _conParams['cntlr'] = self.cntlr
            conn = rssDBConnection(**_conParams)
        if not conn.checkConnection():
            self.cntlr.addToLog(_('No SEC RSS DB Connection'), messageCode="RssDB.Info", file="",  level=logging.INFO)
            self.cntlr.logView.listBox.see(tkr.END)
            return
        elif conn.checkConnection():
            res = conn.searchFilings(**params, getFiles=True)
            saveAs = None
            if self.dbConnection.searchResultsModelXbrl and os.path.isfile(self.dbConnection.searchResultsTempFile):
                # save results to same temp file
                saveAs = self.dbConnection.searchResultsTempFile
            # tmpF = _makeRssFeedLikeXml(conn, res['filings'], res['files'], showcount=False)[0]
            tmpF = _makeRssFeedLikeXml(conn=self.dbConnection, dbFilings_dicts=res['filings'], dbFiles_dicts=res['files'], saveAs=saveAs, showcount=False)[0]
            self.dbConnection.searchResultsTempFile = tmpF
            if hasattr(self.cntlr, 'hasGui'):
                filesource = FileSource.FileSource(tmpF, self.cntlr)
                threading.Thread(target=rssDB_backgroundLoadXbrl, args=(self.cntlr, filesource,False,False, params, self.multiprocessQueue), daemon=True).start()
            elif hasattr(self.cntlr, 'runKwargs'):
                self.cntlr.runKwargs(file=tmpF, keepOpen='')
                ViewWinRssFeed.viewRssFeed(self.cntlr.modelManager.modelXbrl, self.tabWin)
        if hasattr(self, 'queryFrame'):
            self.queryFrame.searchDB_btn.config(state='normal')
        return

    def _destroy(self, event=None):
        self.disconnectDB(destroy=1)
        self.destroy()
        return

    def addPriorDatabaseSettings(self, dbSettings: tuple):
        '''Cache up to 6 entries for database settings'''
        dbSettingsList = list(dbSettings)
        dbSettingsList[3] = '' # don't save password

        # Add last connection to end of list
        if dbSettingsList in self.priorDatabaseSettingsList:
            self.priorDatabaseSettingsList.remove(dbSettingsList)
        # if not dbSettingsList in self.priorDatabaseSettingsList:
        self.priorDatabaseSettingsList.append(dbSettingsList)
        if len(self.priorDatabaseSettingsList) > 6:
            del self.priorDatabaseSettingsList[0]
        self.cntlr.saveConfig()
        return

    def disconnectDB(self, destroy=0, confirm=True):
        global con_dependent_ui
        _confirm = True
        msg = None
        res = True
        if self.dbConnection and self.dbConnection.checkConnection():
            if confirm and len(con_dependent_ui):
                _confirm = tkr.messagebox.askyesno(title=_('Disconnect RSS DB'),
                                                        message=_('Disconnecting from db will close the following windows:\n{}\n\nDo you want to proceed?').format('\n'.join(['- '+x.title() for x in con_dependent_ui])), icon='warning')
            if _confirm:
                while len(con_dependent_ui):
                    con_dependent_ui[0].close()
                conParams = self.dbConnection.conParams
                msg = _('[{}] Disconnected from {}{} - {}').format(datetime.datetime.today().strftime(
                    "%Y-%m-%d %H:%M:%S"), conParams['host'] + '/' if conParams['host'] else '', conParams['database'], self.dbConnection.product)
                self.dbConnection.close()
                chk = self.dbConnection.checkConnection()
                if chk:
                    tkr.messagebox.showwarning(_("RSS DB message"),_("Could not disconnect, Connection still active!"))
                    res = False
                else:
                    try:
                        self.cntlr.addToLog(msg if msg else _('disconnected!'), messageCode="RssDB.Info", file="",  level=logging.INFO)
                        self.cntlr.addToLog('')
                        self.cntlr.logView.listBox.see(tkr.END)
                    except:
                        pass
                if not destroy:
                    if hasattr(self, 'updateDBFrame'):
                        self.updateDBFrame.update_btn.config(state='disabled')
                    if hasattr(self, 'queryFrame'):
                        self.queryFrame.searchDB_btn.config(state='disabled')
                    if hasattr(self, 'btn_updateDB'):
                        self.btn_updateDB.config(state='disabled')
                    if hasattr(self, 'btn_searchDB'):
                        self.btn_searchDB.config(state='disabled')

                    # self.updateDBFrame.update_btn.config(state='disabled')
                    self.btn_disconnectDB.config(state='disabled')
                    self.btn_checkDbStat.config(state='disabled')
                    self.btn_showReport.config(state='disabled')
                    self.connectionIndicatorCanvas.itemconfig(self.connectionIndicator, fill='red')
                    self.connectionIndicatorText['text'] = _('Not Connected to DB')
                    self.btn_connectToDb.config(state='normal')
            else:
                res = False       
        return res

    def backgroundGetDbStats(self, conn):
        if conn:
            try:
                if conn.product == 'sqlite':
                    _conParams = conn.conParams
                    _conParams['cntlr'] = self.cntlr
                    conn = rssDBConnection(**_conParams)
                    # conn.showStatus = self.appendLines
                dbStats = conn.getDbStats()['textResult']
                l = ['Database Stats ({}) [{}]:'.format(dbStats['DatabaseSize'], datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))]
                if dbStats:
                    _text = {'CountFeeds': 'Count of Feeds', 'LatestFeed': 'Latest Feed Month', 'EarliestFeed': 'Earliest Feed Month',
                                'CountFilings': 'Count of Filings', 'LatestFiling':'Latest Filing Publication Date',
                                'EarliestFiling':'Earliest Filing Publication Date', 'CountFilers': 'Count of Filers', 'CountFiles': 'Count of Files', 
                                'missingTables': 'Missing Tables', 'noConnection': 'No DB Connection', 'missingCollections': 'Missing Collections', 'LastUpdate': 'Last Updated', 'DatabaseSize': 'DB Size'}
                    for k, v in dbStats.items():
                        if v.isdigit():
                            l.append('{}: {:,}'.format(_text[k],int(v)))
                        else:
                            l.append('{}: {}'.format(_text[k],v))
                    for _l in l:
                        self.cntlr.addToLog(_l, messageCode="RssDB.Info", file="",  level=logging.INFO)
                    self.cntlr.addToLog('')
                    self.cntlr.logView.listBox.see(tkr.END)
                if conn.product == 'sqlite':
                    conn.close()
                    del conn
                self.cntlr.logView.listBox.see(tkr.END)
            except Exception as e:
                tkr.messagebox.showerror(title='RSS DB Error - getDBStats', message=traceback.format_exc(), parent=self.cntlr.parent)
        else:
            tkr.messagebox.showerror(title='RSS DB Error - getDBStats', message=_('No DB Connection'))
        return

    def connectToDB(self):
        priorSettings = self.priorDatabaseSettingsList[-1] if len(self.priorDatabaseSettingsList) else None
        res = self.askDatabase(self, priorSettings)
        if res and self.runGetStat:
            threading.Thread(target=self.backgroundGetDbStats, args=[self.dbConnection], daemon=True).start()
        if not self.runGetStat:
            self.runGetStat = True
        try:
            self.cntlr.logView.listBox.see(tkr.END)
        except:
            pass
        return

    def askDatabase(self, parent, priorDatabaseSettings):
        res = False
        if isinstance(priorDatabaseSettings, (tuple, list)) and len(priorDatabaseSettings) == 10:
            urlAddr, urlPort, user, password, database, schema, createSchema, createDB, timeout, dbType = priorDatabaseSettings
        else:
            urlAddr = urlPort = user = password = database = schema = createSchema = createDB = timeout = dbType = None
        dialog = DialogRssDBConnect(parent, urlAddr=urlAddr, urlPort=urlPort, user=user, password=password, 
                                       createSchema=createSchema if createSchema else False, 
                                       createDB=createDB if createDB else False,
                                       schema=schema, database=database, timeout=timeout, dbType=dbType, showHost=False, showUrl=True, 
                                       showUser=True, showRealm=False, showDatabase=True)
        if dialog.accepted:
            res = True
        return res

    def showSelections(self):
        global industryCodesSelection
        py = sys.executable
        self.cntlr.addToLog(sys.argv)
        return
 
    def openIndustrySelector(self):
        selector = industrySelector(self, self.btn_industrySelect)
        return

class DialogRssDBConnect(tkr.Toplevel):
    """Based on arelle/DialogUserPassword"""
    def __init__(self, parent:rssDBFrame, urlAddr=None, urlPort=None, user=None, password=None, database=None, 
                 timeout=None, dbType=None, schema=None, createSchema=False, createDB=False,
                 showUrl=False, showUser=False, showHost=True, showRealm=True, showDatabase=False,
                 userLabel=None, passwordLabel=None, hidePassword=True):
        super(DialogRssDBConnect, self).__init__(parent)
        self.parent = parent
        parentGeometry = re.match("(\d+)x(\d+)[+]?([-]?\d+)[+]?([-]?\d+)", parent.parent.master.master.geometry())
        dialogX = int(parentGeometry.group(3))
        dialogY = int(parentGeometry.group(4))
        self.accepted = False
        # self.transient(self.parent)
        self.title(_("Connect to SEC XBRL Filings RSS DB"))
        self.dbTypeVar = tkr.StringVar()
        self.urlAddrVar = tkr.StringVar()
        self.urlAddrVar.set(urlAddr if urlAddr else "")
        self.urlPortVar = tkr.StringVar()
        self.urlPortVar.set(urlPort if urlPort else "")
        self.userVar = tkr.StringVar()
        self.userVar.set(user if user else "")
        self.passwordVar = tkr.StringVar()
        self.passwordVar.set(password if password else "")
        self.databaseVar = tkr.StringVar()
        self.databaseVar.set(database if database else "")
        self.timeoutVar = tkr.StringVar()
        self.timeoutVar.set(timeout if timeout else "")
        self.schemaVar = tkr.StringVar()
        self.schemaVar.set(schema if schema else '')
        self.createSchema = createSchema
        self.createDatabase = createDB
        self.enabledWidgets = []

        
        frame = tkr.LabelFrame(self, text=_("Connection Parameters"))
        y = 0

        dbTypeLabel = tkr.Label(frame, text=_("DB type:"), underline=0, name='dbTypeLabel', width=8, anchor='w')
        dbTypeLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        cbDbType = ttk.Combobox(frame, textvar=self.dbTypeVar, values = DBTypes, state='readonly', name='dbTypeCombobox')
        cbDbType.set(dbType if dbType else DBTypes[0])
        cbDbType.grid(row=y, column=1, columnspan=4, sticky=tkr.EW, pady=3, padx=3)
        self.dbTypeVar.trace('w', lambda x,y,z: self.dbTypeModified(clear=False))
        self.enabledWidgets.append(cbDbType)
        cbDbType.focus_set()
        y += 1

        urlAddrLabel = tkr.Label(frame, text=_("Address:"), underline=0, name='addressLabel',width=8, anchor='w')
        urlAddrEntry = tkr.Entry(frame, textvariable=self.urlAddrVar, name='addressEntry')
        urlPortLabel = tkr.Label(frame, text=_("Port:"), underline=0, name='portLabel')
        urlPortEntry = tkr.Entry(frame, textvariable=self.urlPortVar, name='portEntry', width=10)
        # urlAddrEntry.focus_set()
        urlAddrLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        urlAddrEntry.grid(row=y, column=1, columnspan=2, sticky=tkr.EW, pady=3, padx=3)
        urlPortLabel.grid(row=y, column=3, sticky=tkr.E, pady=3, padx=3)
        urlPortEntry.grid(row=y, column=4, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(urlPortEntry, text=_("Enter URL address and port number \n"
                                        "  e.g., address: 168.1.2.3 port: 8080 \n"
                                        "  or address: proxy.myCompany.com port: 8080 \n"
                                        "  or leave blank to specify no proxy server"), wraplength=360)
        self.enabledWidgets.append(urlAddrEntry)
        self.enabledWidgets.append(urlPortEntry)
        y += 1

        userLabel = tkr.Label(frame, text=userLabel or _("User:"), underline=0, name='userLabel', width=8, anchor='w')
        userEntry = tkr.Entry(frame, textvariable=self.userVar, name='userEntry')
        userLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        userEntry.grid(row=y, column=1, columnspan=4, sticky=tkr.EW, pady=3, padx=3)
        self.enabledWidgets.append(userEntry)
        y += 1

        passwordLabel = tkr.Label(frame, text=passwordLabel or _("Password:"), underline=0, name='passwordLabel',width=8, anchor='w')
        passwordEntry = tkr.Entry(frame, textvariable=self.passwordVar, show=("*" if hidePassword else None), name='passwordEntry')
        passwordLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        passwordEntry.grid(row=y, column=1, columnspan=4, sticky=tkr.EW, pady=3, padx=3)
        self.enabledWidgets.append(passwordEntry)
        y += 1

        urlDatabaseLabel = tkr.Label(frame, text=_("Database:"), underline=0, name='databaseLabel',width=8, anchor='w')
        urlDatabaseEntry = tkr.Entry(frame, textvariable=self.databaseVar, name= 'databaseEntry')
        urlDatabaseLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        urlDatabaseEntry.grid(row=y, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(urlDatabaseEntry, text=_("Enter database name (optional) or leave blank"), wraplength=360)
        self.enabledWidgets.append(urlDatabaseEntry)
        image = tkr.PhotoImage(file=os.path.join(self.parent.cntlr.imagesDir, "toolbarOpenFile.gif"))
        self._image = image
        btn_sqlite_db_file = tkr.Button(frame, image=image, command=self.openSqliteFile, name='sqlFileButton')
        btn_sqlite_db_file.grid(row=y, column=4, sticky=tkr.EW, pady=3, padx=3)
        self.enabledWidgets.append(btn_sqlite_db_file)
        y += 1

        dummyLabelA = tkr.Label(frame, text='', name='databaseLabel',width=8, anchor='w')
        dummyLabelA.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        self.createDatabaseCb = checkbox(frame, 1, y, columnspan=4, text=_("Create Database and Tables (sqlite and mongodb)"))
        self.createDatabaseCb._name ='databaseCreate'
        self.createDatabaseCb.var = self.createDatabase
        self.createDatabaseCb.valueVar.set(1 if createDB else 0)
        if self.dbTypeVar.get() == 'postgres':
            self.createDatabaseCb.config(state='disabled')
            self.createDatabaseCb.valueVar.set(0)
        self.createDatabaseCb.grid(padx=3)
        ToolTip(self.createDatabaseCb, text=_("Whether to database and tables if not existing - only sqlite and mongodb if user has privileges"), wraplength=360)
        self.enabledWidgets.append(self.createDatabaseCb)
        y += 1

        dbSchemaLabel = tkr.Label(frame, text=_("Schema:"), underline=0, name='schemaLabel',width=8, anchor='w')
        dbSchemaEntry = tkr.Entry(frame, textvariable=self.schemaVar, name='schemaEntry')
        if not self.dbTypeVar.get() == 'postgres':
            dbSchemaEntry.config(state='disabled')
        dbSchemaLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        dbSchemaEntry.grid(row=y, column=1, columnspan=4, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(dbSchemaEntry, text=_("(optional) Enter schema name or leave blank defaults to 'rssFeeds' - postgres only"), wraplength=360)
        self.enabledWidgets.append(dbSchemaEntry)

        y+=1
        dummyLabelB = tkr.Label(frame, text='', name='databaseLabel',width=8, anchor='w')
        dummyLabelB.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        self.createSchemaCb = checkbox(frame, 1, y, text=_("Create Schema and Tables"))
        self.createSchemaCb._name ='schemaCreate'
        self.createSchemaCb.var = self.createSchema
        self.createSchemaCb.valueVar.set(1 if createSchema else 0)
        if not self.dbTypeVar.get() == 'postgres':
            self.createSchemaCb.config(state='disabled')
            self.createSchemaCb.valueVar.set(0)
        self.createSchemaCb.grid(padx=(3,10))
        ToolTip(self.createSchemaCb, text=_("Whether to create schema and tables if not exiting - postgres only if user has privileges"), wraplength=360)
        self.enabledWidgets.append(self.createSchemaCb)
        y += 1

        urlTimeoutLabel = tkr.Label(frame, text=_("Timeout:"), underline=0, name='timeoutLabel', width=8, anchor='w')
        urlTimeoutEntry = tkr.Entry(frame, textvariable=self.timeoutVar, name='timeoutEntry')
        urlTimeoutLabel.grid(row=y, column=0, sticky=tkr.W, pady=0, padx=3)
        urlTimeoutEntry.grid(row=y, column=1, columnspan=4, sticky=tkr.EW, pady=0, padx=3)
        ToolTip(urlTimeoutEntry, text=_("Enter timeout seconds (optional) or leave blank for default (60 secs.)"), wraplength=360)
        self.enabledWidgets.append(urlTimeoutEntry)
        y += 1

        clearButton = tkr.Button(frame, text = _("Clear"), command= self.setDialogueEntries)
        removeFromCacheButton = tkr.Button(frame, text = _("Remove"), command= self.clearCachedConns)
        clearCacheButton = tkr.Button(frame, text = _("Clear Cache"), command= lambda: self.clearCachedConns(False))
        prevButton = tkr.Button(frame, text = _("< Previous"), command= lambda: self.browseDbSettings(-1), width=8, anchor='w')
        nextButton = tkr.Button(frame, text = _("Next >"), command= lambda: self.browseDbSettings(1))
        prevButton.grid(row=y, column=0, sticky=tkr.EW, pady=0, padx=3)
        clearButton.grid(row=y, column=2, sticky=tkr.EW, pady=3,padx=(3,1))
        removeFromCacheButton.grid(row=y, column=3, sticky=tkr.EW, pady=3,padx=(3,1))
        clearCacheButton.grid(row=y, column=1, sticky=tkr.EW, pady=3,padx=(3,1))
        nextButton.grid(row=y, column=4, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(clearButton, text=_("Clear Form"), wraplength=360)
        ToolTip(clearCacheButton, text=_("Removes all cached connections params"), wraplength=360)
        ToolTip(removeFromCacheButton, text=_("Remove current entry from cache"), wraplength=360)
        y += 1

        okButton = tkr.Button(frame, text=_("Connect"), command=self.ok)
        cancelButton = tkr.Button(frame, text=_("Cancel"), command=self.close)
        cancelButton.grid(row=y, column=3, sticky=tkr.E, pady=3, padx=3)
        okButton.grid(row=y, column=4, sticky=tkr.EW, pady=3, padx=3)
    
        # showWidz = tkr.Button(frame, text=_("Show Widgets"), command=self.showWid)
        # showWidz.grid(row=y, column=0, sticky=tkr.EW, pady=3, padx=3)


        frame.grid(row=0, column=0, sticky=(tkr.N,tkr.S,tkr.E,tkr.W), padx=3, pady=3)
        # frame.columnconfigure(1, weight=1)
        self.__frame = frame
        self.__namedWidgets = [x for x in frame.winfo_children() if not x._name.startswith('!')]

        window = self.winfo_toplevel()
        # window.columnconfigure(0, weight=1)
        self.geometry("+{0}+{1}".format(dialogX+50,dialogY+100))
        makeWeight(self)
        makeWeight(frame)
        frame.columnconfigure(4, weight=0)
        frame.columnconfigure(0, weight=0)
        # self.resizable(True, True)
        
        self.bind("<Return>", self.ok)
        self.bind("<Escape>", self.close)
        
        self.protocol("WM_DELETE_WINDOW", self.close)
        self.grab_set()
        self.dbTypeModified(clear=False)
        self.wait_window(self)

    def showWid(self):
        # testing
        # print(self.__namedWidgets)
        # print([x._name for x in self.__namedWidgets])
        pass

    def openSqliteFile(self):
        fileName = filedialog.askopenfilename(parent=self,
                title=_("Open Sqlite File"),
                initialdir=".",
                filetypes=[(("Sqlite db files"), ".db .sdb .sqlite .db3 .s3db .sqlite3 .sl3 .db2 .s2db .sqlite2 .sl2")])
        if fileName:
            dbEntry =  [x for x in self.__namedWidgets if x._name == 'databaseEntry'][0]
            dbEntry.delete(0, tkr.END)
            dbEntry.insert(0, fileName)
        return

    def dbTypeModified(self, clear=True, i=None, v=None, o=None):
        widgets = {'dbTypeLabel':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'dbTypeCombobox':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'addressLabel':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'addressEntry':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'portLabel':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'portEntry':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'userLabel':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'userEntry':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'passwordLabel':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'},
                    'passwordEntry':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'normal'}, 
                    'databaseLabel':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'databaseEntry':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'databaseCreate':{'postgres': 'disabled', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'sqlFileButton':{'postgres': 'disabled', 'sqlite': 'normal', 'mongodb':'disabled'}, 
                    'schemaLabel':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'disabled'}, 
                    'schemaEntry':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'disabled'}, 
                    'schemaCreate':{'postgres': 'normal', 'sqlite': 'disabled', 'mongodb':'disabled'}, 
                    'timeoutLabel':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}, 
                    'timeoutEntry':{'postgres': 'normal', 'sqlite': 'normal', 'mongodb':'normal'}}
        
        for w in self.__namedWidgets:
            wid_name = getattr(w, '_name')
            db_Selection = self.dbTypeVar.get()
            # clear schema entries
            if type(w).__name__ == 'checkbox' and widgets[wid_name][db_Selection] == 'disabled':
                w.valueVar.set(0)
                w.var = False
            elif type(w).__name__ == 'Entry' and widgets[wid_name][db_Selection] == 'disabled':
                    w.delete(0, 'end')
                    w.insert(0, '')

            if clear:
                if type(w).__name__ == 'Entry':
                    w.delete(0, 'end')
                    w.insert(0, '')
                elif type(w).__name__ == 'checkbox':
                    w.valueVar.set(0)
                    w.var = False
            w.config(state=widgets[wid_name][db_Selection])
        return

    def checkEntries(self):
        errors = []
        if self.urlPort and not self.urlPort.isdigit():
            errors.append(_("Port number invalid"))
        if self.timeout and not self.timeout.isdigit():
            errors.append(_("Timeout seconds invalid"))
        if hasattr(self,"cbDbType") and self.cbDbType.value not in DBDescriptions:
            errors.append(_("DB type is invalid"))
        if errors:
            tkr.messagebox.showwarning(_("Dialog validation error(s)"),
                                "\n ".join(errors), parent=self)
            return False
        return True

    def setDialogueEntries(self, dbSettings = None):
        if not dbSettings or not len(dbSettings) == 10:
            dbSettings = ['', '', '', '', '', '', False, False, '', 'postgres']
        urlAddr, urlPort, user, password, database, schema, createSchema, createDB, timeout, dbType = dbSettings

        for w in self.__namedWidgets:
            if w._name == 'dbTypeCombobox': 
                w.set(dbType if dbType else DBTypes[0])
                self.dbTypeModified()
            elif w._name == 'addressEntry':
                w.delete(0, 'end')
                w.insert(0, urlAddr)
            elif w._name == 'portEntry':
                w.delete(0, 'end')
                w.insert(0, urlPort)
            elif w._name == 'userEntry':
                w.delete(0, 'end')
                w.insert(0, user)
            elif w._name == 'passwordEntry':
                w.delete(0, 'end')
                w.insert(0, password)
            elif w._name == 'databaseEntry':
                w.delete(0, 'end')
                w.insert(0, database)
            elif w._name == 'schemaEntry':
                w.delete(0, 'end')
                w.insert(0, schema)
            elif w._name == 'schemaCreate':
                w.valueVar.set(1 if createSchema else 0)
            elif w._name == 'databaseCreate':
                w.valueVar.set(1 if createDB else 0)
            elif w._name == 'timeoutEntry':
                w.delete(0, 'end')
                w.insert(0, timeout)
        return

    def clearCachedConns(self, currentOnly = True):
        # global priorSettingsFile
        lstLen = len(self.parent.priorDatabaseSettingsList)
        if lstLen:
            if currentOnly:
                num = self.parent.currentSettingSelectionIndex
                if num <= len(self.parent.priorDatabaseSettingsList):
                    del self.parent.priorDatabaseSettingsList[num]
            else:
                self.parent.priorDatabaseSettingsList = []
                self.parent.cntlr.config['rssDBconnection'] = []
                self.parent.cntlr.saveConfig()
           
        self.setDialogueEntries()
        return
 
    def browseDbSettings(self, direction):
        if not direction: # clears current form
            self.setDialogueEntries()
            return
        if not len(self.parent.priorDatabaseSettingsList): # nothing to do
            return

        if not direction in (-1, 1) or direction > 0 :
            direction = 1
        elif direction < 0:
            direction = -1

        nextIndex = self.parent.currentSettingSelectionIndex + direction
        settingLastIndex = len(self.parent.priorDatabaseSettingsList)-1
        selectedIndex = nextIndex 

        if nextIndex > settingLastIndex:
            selectedIndex = 0 # return to begining
        elif nextIndex < 0:
            selectedIndex = settingLastIndex # recycle 

        # print(selectedIndex)
        
        self.setDialogueEntries(self.parent.priorDatabaseSettingsList[selectedIndex])
        self.parent.currentSettingSelectionIndex = selectedIndex

        return
   
    def ok(self, event=None):
        if hasattr(self, "useOsProxyCb"):
            self.useOsProxy = self.useOsProxyCb.value
        self.urlAddr = self.urlAddrVar.get()
        if self.urlAddr.startswith("http://"): self.urlAddr = self.ulrAddr[7:] # take of protocol part if any
        self.urlPort = self.urlPortVar.get()
        self.user = self.userVar.get()
        self.password = self.passwordVar.get()
        self.database = self.databaseVar.get()
        self.timeout = self.timeoutVar.get()
        self.dbType = self.dbTypeVar.get()
        self.schema = self.schemaVar.get()
        self.createSchema = self.createSchemaCb.value if self.createSchemaCb else False
        self.createDatabase = self.createDatabaseCb.value if self.createDatabaseCb else False
        if not self.checkEntries():
            return
        try:
            self.getDbConnection()
        except:
            pass
        
        if self.accepted:
            self.close()

        return

    def backgroundCreateDB(self, conn, product):
        try:
            if product == 'sqlite':
                _conParams = conn.conParams
                _conParams['cntlr'] = self.parent.cntlr
                _con = rssDBConnection(**_conParams)
                _con.verifyTables(createTables=True, dropPriorTables=False)
                _con.close()
                del _con
            if product == 'postgres':
                conn.verifyTables(createTables=True, dropPriorTables=False)
            elif product == 'mongodb':
                conn.verifyCollections(createCollections=True, dropPriorCollections=False)
            self.parent.backgroundGetDbStats(conn)
        except Exception as e:
            tkr.messagebox.showerror(title='RSS DB Error - createDB', message=traceback.format_exc())
        return

    def getDbConnection(self):
        dbParamsInput = (self.urlAddr, self.urlPort, self.user, self.password, self.database, self.schema, 
                            self.createSchema, self.createDatabase, self.timeout, self.dbType)
        conParams = {"host": dbParamsInput[0],
                    "port": int(dbParamsInput[1]) if dbParamsInput[1] else None, 
                    "user": dbParamsInput[2], 
                    "password": dbParamsInput[3], 
                    "database": dbParamsInput[4], 
                    "schema": dbParamsInput[5], 
                    "createSchema": dbParamsInput[6],
                    "createDB": dbParamsInput[7],
                    "timeout": int(dbParamsInput[8]) if dbParamsInput[8] else None, 
                    "product": dbParamsInput[9]}

        _cntlr = self.parent.cntlr
        
        dbConnection = None
        chk = False
        try:
            dbConnection = rssDBConnection(cntlr=_cntlr, **conParams)
            dbConnection.searchResultsModelXbrl = None
            dbConnection.searchResultsTempFile = ''
            dbConnection.rssDBFrame = self.parent
            if conParams['createSchema'] or conParams['createDB']:
                self.parent.runGetStat = False
                threading.Thread(target=self.backgroundCreateDB, args=[dbConnection, conParams['product']], daemon=True).start()
            self.parent.disconnectDB() # close previous connection
            self.parent.cntlr.addToLog(_("[{}] connected to {}{} - {}").format(datetime.datetime.today().strftime(
                "%Y-%m-%d %H:%M:%S"), dbConnection.conParams['host'] + '/' if dbConnection.conParams['host'] else '', dbConnection.conParams['database'], dbConnection.product))
            self.parent.cntlr.addToLog('')
            self.parent.cntlr.logView.listBox.see(tkr.END)
            self.parent.addPriorDatabaseSettings(dbParamsInput)
            self.parent.dbConnection = dbConnection
            _cntlr.dbConnection = dbConnection
            self.master.btn_disconnectDB.config(state='normal')
            self.master.btn_checkDbStat.config(state='normal')
            self.master.btn_showReport.config(state='normal')
            if hasattr(self.master, 'updateDBFrame'):
                self.master.updateDBFrame.update_btn.config(state='normal')
            if hasattr(self.master, 'queryFrame'):
                self.master.queryFrame.searchDB_btn.config(state='normal')
            if hasattr(self.master, 'btn_updateDB'):
                self.master.btn_updateDB.config(state='normal')
            if hasattr(self.master, 'btn_searchDB'):
                self.master.btn_searchDB.config(state='normal')

            self.accepted = True
            self.parent.connectionIndicatorCanvas.itemconfig(self.parent.connectionIndicator, fill='green')
            self.parent.connectionIndicatorText['text'] = _("Connected to {}{} - {}").format(dbConnection.conParams['host'] + '/' if dbConnection.conParams['host'] else '', dbConnection.conParams['database'], dbConnection.product)
            self.parent.btn_connectToDb.config(state='disabled')
        except Exception as e:
            if dbConnection:
                dbConnection.close()
            tkr.messagebox.showerror(_('RSS DB Error'), str(e))
        return

    def close(self, event=None):
        self.parent.focus_set()
        self.destroy()
        return

class runFormulaDialog(tkr.Toplevel):
    def __init__(self, searchResView, cntlr, master, selectionButton: tkr.Button, includeRun=False, **kw):
        global con_dependent_ui
        super().__init__(master, **kw)
        self.isSaved = False
        self.parent = master
        self.cntlr = cntlr
        self.con = cntlr.dbConnection
        self.searchResView = searchResView

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        # variables
        self.addFormulaFileVar = tkr.StringVar()
        self.addFormulaFileVar.set('')
        self.addFormulaFileVar.trace('w', lambda x,y,z: self.addFormulaFileModified(clear=False))

        self.selectedFormula = tkr.IntVar()
        self.selectedFormula.trace('w', lambda x,y,z: self.formulaSelectionModified())

        self.removeFormulaVar = tkr.StringVar()
        self.removeFormulaVar.set('')
        self.removeFormulaVar.trace('w', lambda x,y,z: self.removeFormulaModified())

        self.saveFolderVar = tkr.StringVar()
        self.saveFolderVar.set('')

        self.runFormulaFileVar = tkr.StringVar()
        self.runFormulaFileVar.set('')

        self.runFormulaFileFolderVar = tkr.StringVar()
        self.runFormulaFileFolderVar.set('')

        self.additionalImportVar = tkr.StringVar()
        self.additionalImportVar.set('')

        # Settings
        self.selectionButton = selectionButton
        self.selectionButton.config(state='disabled')
        self.wm_title('Rss DB Formula Selection')
        # self.geometry('600x300')
        self.protocol('WM_DELETE_WINDOW', self.close)

        # Formulae list
        self.frame_tree = ttk.Notebook(self, height=100) #tkr.Frame(self)
        self.tree = ViewTree(cntlr.modelManager.modelXbrl, self.frame_tree, 'Formulae In DB', hasToolTip=False, lang=None)
        self.frame_tree.grid(row=0, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W))

        # remove view from mX views
        self.tree.modelXbrl.views.remove(self.tree)

        # Outter Formulae options frame
        self.frame_opts = tkr.LabelFrame(self, text=_("Formula Options"))
        self.frame_opts.grid(row=1, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=3, pady=3)
       
        # Add remove formulae frame
        self.frame_opts_addRm = tkr.LabelFrame(self.frame_opts, text=_("Add/Remove Formula(e)"))
        self.frame_opts_addRm.grid(row=0, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=3, pady=3)
        
        addFormulaFileLabel = tkr.Label(self.frame_opts_addRm, text=_("Add Formula File:"), underline=0, name='addFormulaFileLabel')
        addFormulaFileLabel.grid(row=0, column=0, sticky=tkr.W, pady=3, padx=3)

        self.addFormulaFileEntry = tkr.Entry(self.frame_opts_addRm, textvariable=self.addFormulaFileVar, width=25, name='addFormulaFileEntry')
        self.addFormulaFileEntry.grid(row=0, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.addFormulaFileEntry, text=_("Enter File path for Formula Linkbase to add to DB"), wraplength=360)
        
        self.image_f = tkr.PhotoImage(file=os.path.join(self.cntlr.imagesDir, "toolbarOpenFile.gif"))
        self.btn_formula_add_file = tkr.Button(self.frame_opts_addRm, image=self.image_f,
                                                command=lambda: self.runFormulaOpenFileGUI(self.addFormulaFileEntry, _("Select Formula Linkbase File"), [(_("XBRL formula Linkbase"), ".XML .xml")]), 
                                                name='btn_formula_add_file')
        self.btn_formula_add_file.grid(row=0, column=4, sticky=tkr.EW, pady=3, padx=3)
        
        self.btn_formula_add_actions = tkr.Button(self.frame_opts_addRm, text='Add To DB',command=self.addFormulaGUI, name='btn_formula_add_actions')
        self.btn_formula_add_actions.grid(row=0, column=5, sticky=tkr.EW, pady=3, padx=3)
        self.btn_formula_add_actions.config(state='disabled')
        ToolTip(self.btn_formula_add_actions, text=_("Add formula to DB from selected file"), wraplength=360)

        removeFormulaLabel = tkr.Label(self.frame_opts_addRm, text=_("Remove Formula(e):"), underline=0, name='removeFormulaLabel')
        removeFormulaLabel.grid(row=1, column=0, sticky=tkr.W, pady=3, padx=3)

        self.removeFormulaEntry = tkr.Entry(self.frame_opts_addRm, textvariable=self.removeFormulaVar, width=25, name='removeFormulaEntry')
        self.removeFormulaEntry.grid(row=1, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.removeFormulaEntry, text=_("Enter comma separated formulaId(s) for formula(e) to remove"), wraplength=360)

        self.getSelected_cb = checkbox(self.frame_opts_addRm, 4, 1, columnspan=1, text=_("Get Selected Id"))
        self.getSelected_cb.name ='getSelected_cb'
        self.getSelected_cb.valueVar.set(0)
        self.getSelected_cb.grid(padx=0)
        ToolTip(self.getSelected_cb, text=_("When enabled, selection is included with formula(e) to remove"), wraplength=360)

        self.btn_formula_remove_actions = tkr.Button(self.frame_opts_addRm, text='Remove from DB', command=self.removeFormulaGUI, name='btn_formula_remove_actions')
        self.btn_formula_remove_actions.grid(row=1, column=5, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.btn_formula_remove_actions, text=_("Removes the formula currently selected from db"), wraplength=360)
        self.btn_formula_remove_actions.config(state='disabled')

        # Run formula from db options frame
        self.frame_opts_run_db = tkr.LabelFrame(self.frame_opts, text=_("Run Formula from DB"))
        self.frame_opts_run_db.grid(row=1, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=3, pady=3)

        runFormulaDBLabel = tkr.Label(self.frame_opts_run_db, text=_("Selected Formula:"), underline=0, name='runFormulaDBLabel')
        runFormulaDBLabel.grid(row=0, column=0, sticky=tkr.W, pady=3, padx=1)
        
        self.selectedFormula_entry = tkr.Label(self.frame_opts_run_db, text='', name='selectedFormula_entry', bg='white', fg='black', width=5)
        self.selectedFormula_entry.grid(row=0, column=1, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.selectedFormula_entry, text=_("Selected Formula from the list above to run with selected search results"), wraplength=360)

        additionalImportsLabel = tkr.Label(self.frame_opts_run_db, text=_("Import:"), underline=0, name='additionalImports')
        additionalImportsLabel.grid(row=1, column=0, sticky=tkr.W, pady=3, padx=1)
        self.additionalImportEntry = tkr.Entry(self.frame_opts_run_db, textvariable=self.additionalImportVar, width=30, name='additionalImportsEntry')
        self.additionalImportEntry.grid(row=1, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=1)
        ToolTip(self.additionalImportEntry, text=_("Additional files to import (schemas, custom function implementations...)"), wraplength=360)
        self.btn_formula_imports = tkr.Button(self.frame_opts_run_db, image=self.image_f,
                                                    command= self.btn_cmd_formula_imports, 
                                                    name='btn_formula_imports')
        self.btn_formula_imports.grid(row=1, column=4, sticky=tkr.EW, pady=3, padx=1)


        self.insertResultsIntoDB_cb = checkbox(self.frame_opts_run_db, 2, 0, columnspan=1, text=_("Insert Results"))
        self.insertResultsIntoDB_cb.name ='insertResultsIntoDB_cb'
        self.insertResultsIntoDB_cb.valueVar.set(0)
        self.insertResultsIntoDB_cb.grid(padx=0)
        ToolTip(self.insertResultsIntoDB_cb, text=_("Whether to insert the results of running the formula into DB"), wraplength=360)

        self.updateExistingResults_cb = checkbox(self.frame_opts_run_db, 3, 0, columnspan=1, text=_("Update Existing"))
        self.updateExistingResults_cb.name ='updateExistingResults_cb'
        self.updateExistingResults_cb.valueVar.set(0)
        self.updateExistingResults_cb.grid(padx=0)
        ToolTip(self.updateExistingResults_cb, text=_("Whether to update result in DB if a result exist for running the same formula with the same filing"), wraplength=360)

        self.saveResultsToFolder_cb = checkbox(self.frame_opts_run_db, 4, 0, columnspan=1, text=_("Save To File"))
        self.saveResultsToFolder_cb.name ='saveResultsToFolder_cb'
        self.saveResultsToFolder_cb.valueVar.set(0)
        self.saveResultsToFolder_cb.grid(padx=0)
        ToolTip(self.saveResultsToFolder_cb, text=_("Whether to save result of running formula for each filing as an xml file, if selected a "
                                                    "path to a folder MUST be selected below, this can be selected along with insert into db option"), wraplength=360)

        saveFolderLabel = tkr.Label(self.frame_opts_run_db, text=_("Save To Folder:"), underline=0, name='saveFolderLabel')
        saveFolderLabel.grid(row=2, column=0, sticky=tkr.W, pady=3, padx=1)
        self.saveFolderEntry = tkr.Entry(self.frame_opts_run_db, textvariable=self.saveFolderVar, width=30, name='saveFolderEntry')
        self.saveFolderEntry.grid(row=2, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=1)
        ToolTip(self.saveFolderEntry, text=_("Enter Folder path to save formula output files"), wraplength=360)
        self.btn_formula_run_folder = tkr.Button(self.frame_opts_run_db, image=self.image_f,
                                                    command= lambda: self.runFormulaOpenFolderGUI(self.saveFolderEntry, _("Select folder to save formula output")), 
                                                    name='btn_formula_run_folder')
        self.btn_formula_run_folder.grid(row=2, column=4, sticky=tkr.EW, pady=3, padx=1)

        
        self.btn_run_db = tkr.Button(self.frame_opts_run_db, text='RUN' if includeRun else 'SAVE', \
                                        command=self.runFormulaFromDb if includeRun else self.btn_cmd_OK , name='btn_run_db')
        self.btn_run_db.grid(row=0, column=5, rowspan=3, sticky=tkr.NSEW, pady=3, padx=1)

        # # Run formula from file options frame
        # self.frame_opts_run_file = tkr.LabelFrame(self.frame_opts, text=_("Run Formula from File (Can not insert result to db)"))
        # self.frame_opts_run_file.grid(row=3, column=0, sticky=(tkr.N, tkr.S, tkr.E, tkr.W), padx=3, pady=3)

        # runFormulaFileLabel = tkr.Label(self.frame_opts_run_file, text=_("Formula Linkbase:"), underline=0, name='runFormulaFileLabel')
        # runFormulaFileLabel.grid(row=0, column=0, sticky=tkr.W, pady=3, padx=3)

        # self.runFormulaFileEntry = tkr.Entry(self.frame_opts_run_file, textvariable=self.runFormulaFileVar, width=25, name='runFormulaFileEntry')
        # self.runFormulaFileEntry.grid(row=0, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=3)
        # ToolTip(self.runFormulaFileEntry, text=_("Enter File path for Formula Linkbase to run"), wraplength=360)

        # self.btn_formula_run_file_file = tkr.Button(self.frame_opts_run_file, image=self.image_f,
        #                                         command= lambda: self.runFormulaOpenFileGUI(self.runFormulaFileEntry, _('Select Formula linkbase to run'), [(_("XBRL formula Linkbase"), ".XML .xml")]), 
        #                                         name='btn_formula_run_file_file')
        # self.btn_formula_run_file_file.grid(row=0, column=4, sticky=tkr.EW, pady=3, padx=3)

        # runFormulaFileFolderLabel = tkr.Label(self.frame_opts_run_file, text=_("Save To Folder:"), underline=0, name='runFormulaFileFolderLabel')
        # runFormulaFileFolderLabel.grid(row=1, column=0, sticky=tkr.W, pady=3, padx=3)

        # self.runFormulaFileFolderEntry = tkr.Entry(self.frame_opts_run_file, textvariable=self.runFormulaFileFolderVar, width=25, name='runFormulaFileFolderEntry')
        # self.runFormulaFileFolderEntry.grid(row=1, column=1, columnspan=3, sticky=tkr.EW, pady=3, padx=3)
        # ToolTip(self.runFormulaFileFolderEntry, text=_("Enter File path for Formula Linkbase to run"), wraplength=360)

        # self.btn_formula_run_file_folder = tkr.Button(self.frame_opts_run_file, image=self.image_f,
        #                                     command=lambda: self.runFormulaOpenFolderGUI(self.runFormulaFileFolderEntry, _("Select folder to save formula output")), 
        #                                     name='btn_formula_run_file_folder')
        # self.btn_formula_run_file_folder.grid(row=1, column=4, sticky=tkr.EW, pady=3, padx=3)


        # self.btn_run_file = tkr.Button(self.frame_opts_run_file, text='RUN',command=self.runFormulaFileGUI, name='btn_run_file')
        # self.btn_run_file.grid(row=0, column=5, rowspan=2, sticky=tkr.NSEW, pady=3, padx=1)



        ##
        opts_cols, opts_rows = self.frame_opts.grid_size()
        for c in range(0, opts_cols):
            self.frame_opts.columnconfigure(c, weight=1)
            
        for r in range(0, opts_rows):
            self.frame_opts.rowconfigure(r, weight=1)

        ##
        addRm_cols, addRms_rows = self.frame_opts_addRm.grid_size()
        self.frame_opts_addRm.rowconfigure(0, weight=1)
        
        for c in range(0, addRm_cols):
            self.frame_opts_addRm.columnconfigure(c, weight=1)

        ##
        opts_run_cols, opts_run_rows = self.frame_opts_run_db.grid_size()
        for c in range(0, opts_run_cols):
            self.frame_opts_run_db.columnconfigure(c, weight=1)
            
        for r in range(0, opts_run_rows):
            self.frame_opts_run_db.rowconfigure(r, weight=1)

        ##
        # opts_run_file_cols, opts_run_file_rows = self.frame_opts_run_file.grid_size()
        # for c in range(0, opts_run_file_cols):
        #     self.frame_opts_run_file.columnconfigure(c, weight=1)
            
        # for r in range(0, opts_run_file_rows):
        #     self.frame_opts_run_file.rowconfigure(r, weight=1)
                
        self.tree.treeView.bind("<<TreeviewSelect>>", self.setSelection)
        self.makeCols()
        self.view()

        con_dependent_ui.append(self)
        self.grab_set()
        if not includeRun:
            self.wait_window(self)

    # def runFormulaFileGUI(self):
    #     fileName = self.runFormulaFileVar.get()
    #     folderPath = self.runFormulaFileFolderVar.get()
    def btn_cmd_formula_imports(self):
        _imports = filedialog.askopenfilenames(title=_('Select additional files to import'), parent=self, filetypes=[(("XML"), ".xml .XML .xsd .XSD")])
        additionalImports = '|'.join(_imports)
        if additionalImports:
            self.additionalImportVar.set(additionalImports)
        return

    def btn_cmd_OK(self):
        self.isSaved = True
        self.parent.focus()
        self.close()
        return
   
    def background_runFormula(self, _key):
        global MAKEDOTS_RSSDBPANEL
        # disable another run
        self.btn_run_db.config(state='disabled')
        # self.btn_run_file.config(state='disabled')

        # disable selections
        self.searchResView.blockSelectEvent = self.searchResView.blockSelectEvent = 1

        _formulaId = self.selectedFormula_entry.cget('text') 
        formulaId = _formulaId if _formulaId else None
        insertRes = self.insertResultsIntoDB_cb.value
        updateExisting = self.updateExistingResults_cb.value
        saveToFolder = self.saveResultsToFolder_cb.value
        folderPath = self.saveFolderVar.get()
        additionalImports = self.additionalImportVar.get()
        
        con = self.con
        closeCon = False
        if con.product == 'sqlite':
            params = con.conParams.copy()
            params['cntlr'] = self.cntlr
            con = rssDBConnection(**params)
            closeCon = True

        ids = self.searchResView.treeView.selection()
        pubDateRssItems = []
        _items = [self.searchResView.modelXbrl.modelObject(x) for x in ids]
        for rssItem in _items:
            pubDateRssItems.append((rssItem.pubDate, rssItem))
        
        sortedItems = sorted(pubDateRssItems, key=lambda x:x[0], reverse=True)
        sortedRssItems = [x[1] for x in sortedItems]
        try:
            res = runFormulaFromDBonRssItems(conn=con, rssItems=sortedRssItems, formulaId=formulaId, additionalImports=additionalImports,
                                                insertResultIntoDb=insertRes, updateExistingResults=updateExisting,
                                                saveResultsToFolder=saveToFolder, folderPath=folderPath, returnResults=False)

        except Exception as e:
            if con.product == 'postgres':
                con.rollback()
            self.cntlr.addToLog(_('Error while processing Formula:\n{}').format(str(e)), messageCode="arellepyError", file=con.conParams['database'], level=logging.ERROR)
        
        MAKEDOTS_RSSDBPANEL[_key] = False
        self.btn_run_db.config(state='normal')
        # self.btn_run_file.config(state='normal')
        self.searchResView.blockSelectEvent = self.searchResView.blockSelectEvent = 0
        if closeCon:
            con.close()  

        return
    
    def runFormulaFromDb(self):
        global MAKEDOTS_RSSDBPANEL
        _key = 'runFormula'
        MAKEDOTS_RSSDBPANEL[_key] = True
        t = threading.Thread(target=self.background_runFormula, args=(_key,), daemon=True)
        t2 = threading.Thread(target=dotted, args=(self.cntlr, _key, 'Running formula'), daemon=True)
        t2.start()
        t.start()
        return
    
    def runFormulaOpenFileGUI(self, entry, title, types):
        fileName = filedialog.askopenfilename(title=title, parent=self, filetypes=types)
        if fileName:
            entry.delete(0, tkr.END)
            entry.insert(0, fileName)
        return

    def runFormulaOpenFolderGUI(self, entry, title):
        dirName = filedialog.askdirectory(title=title, parent=self)
        if dirName:
            entry.delete(0, tkr.END)
            entry.insert(0, dirName)
        return

    def setSelection(self, s):
        focused = self.tree.treeView.focus()
        self.selectedFormula.set(focused)
        self.selectedFormula_entry.config(text=self.selectedFormula.get())
        return

    def formulaSelectionModified(self):
        toRemove = self.removeFormulaEntry.get().split(', ') if self.removeFormulaEntry.get() else []
        getIt = self.getSelected_cb.value
        toInclude = str(self.selectedFormula.get()) if self.selectedFormula.get() else False
        if toInclude:
            if not toInclude in toRemove and getIt:
                toRemove.append(toInclude)
            self.removeFormulaEntry.delete(0, tkr.END)
            self.removeFormulaEntry.insert(0, ', '.join(toRemove))
        return

    def removeFormulaModified(self):
        toRemove = self.removeFormulaVar.get()
        if self.removeFormulaVar.get():
            self.btn_formula_remove_actions.config(state='normal')
        else:
            self.btn_formula_remove_actions.config(state='disabled')
        return

    def addFormulaFileModified(self, clear=True):
        if not self.addFormulaFileVar.get():
            self.btn_formula_add_actions.config(state='disabled')
        else:
            self.btn_formula_add_actions.config(state='normal')
        return

    def addFormulaGUI(self):
        if self.con.checkConnection():
            fileName = self.addFormulaFileVar.get()
            if fileName:
                askDescription = simpledialog.askstring("Input formula description", 
                                                        _("Optional - Add description for the formula file (if left empty, file name will be used)"),
                                                        parent=self)
                fDescription = askDescription if askDescription else os.path.basename(fileName)
                self.cntlr.addToLog(_("Adding file {} to formulae table, Described as {}").format(fileName, fDescription))
                resF = self.con.addFormulaToDb(fileName=fileName, description=fDescription)
                self.view()
        else:
            messagebox.showinfo(_("RSS DB Info"), _("There is no active connection to DB"), icon='warning')
        return

    def removeFormulaGUI(self):
        if self.con.checkConnection():
            _fSelected = self.removeFormulaVar.get()
            fSelected = [int(x) for x in _fSelected.split(', ')]
            if fSelected:
                self.cntlr.addToLog(_("Removing formula(e) id(s) {} from formulae").format(_fSelected))
                confirm = messagebox.askyesno(_('Confirm Removing Formula'),  _('Confirm Removing formulaIds {}?').format(_fSelected), parent=self)
                if confirm:
                    self.con.removeFormulaFromDb(fSelected)
                    self.view()
        else:
            messagebox.showinfo(_("RSS DB Info"), _("There is no active connection to DB"), icon='warning')
        return
        
    def makeCols(self):
        self.tree.treeView["columns"] = ("description", 'dateTimeAdded', 'fileName')
        self.tree.treeView.column("#0", width=3, anchor="w")
        self.tree.treeView.heading("#0", text="Formula ID", anchor="w")
        self.tree.treeView.column("description", width=20, anchor="w")
        self.tree.treeView.heading("description", text="Description", anchor="w")
        self.tree.treeView.column("dateTimeAdded", width=15, anchor="w")
        self.tree.treeView.heading("dateTimeAdded", text="Date Added", anchor="w")
        self.tree.treeView.column("fileName", width=100, anchor="w")
        self.tree.treeView.heading("fileName", text="File Name", anchor="w")
        return

    def view(self): # reload view
        self.selectedFormula.set(0)
        self.tree.setColumnsSortable(startUnsorted=True)
        self.tree.clearTreeView()
        chk = self.con.checkConnection()
        if chk:
            dbFormulae = self.con.getFormulae()
            self.viewFormulae(dbFormulae)
        else:
            messagebox.showinfo(_("RSS DB Info"), _("There is no active connection to DB"), icon='warning')
        return

    def close(self):
        global con_dependent_ui
        del self.tree.viewFrame.view
        if self.tree.modelXbrl:
            self.tree.tabWin.forget(self.tree.viewFrame)
            # self.tree.modelXbrl.views.remove(self.tree)
            self.tree.modelXbrl = None
            self.tree.view = None
        con_dependent_ui.remove(self)
        self.destroy()
        self.selectionButton.config(state='normal')
        return
        
    def viewFormulae(self, formulaeDicts):
        self.id = 1
        for f in formulaeDicts:
            node = self.tree.treeView.insert("", "end", f['formulaId'],
                                        text=(f['formulaId']),
                                        tags=("odd" if self.id & 1 else "even",))
            self.tree.treeView.set(node, "description", f['description'])
            self.tree.treeView.set(node, "dateTimeAdded", f['dateTimeAdded'])
            self.tree.treeView.set(node, "fileName", os.path.basename(f['fileName'])  )
            self.id += 1
        else:
            pass
        return

class industrySelector(tkr.Toplevel):
    def __init__(self, master, selectionButton: tkr.Button, res=res, **kw):
        super().__init__(master, **kw)
        self.selectionButton = selectionButton
        self.skipSelection = False # helper to avoid selection when expanding/collapsing tree
        self.selectionButton.config(state='disabled')
        self.wm_title('Select Industry')
        self.protocol('WM_DELETE_WINDOW', self.closeAction)
        self.frame_tree = tkr.Frame(self)
        self.tree = ttk.Treeview(self.frame_tree, selectmode='none')
        self.tree.heading("#0", text="SEC Industry Classifications")
        self.vScroll = ttk.Scrollbar(self.frame_tree, orient=tkr.VERTICAL, command=self.tree.yview)
        # self.hScroll = ttk.Scrollbar(self.frame_tree, orient=tkr.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vScroll.set) #, xscrollcommand=self.hScroll.set
        # avoid changing selection on expand/collapse
        self.tree.bind("<ButtonRelease-1>", self.select)
        self.tree.bind("<Double-1>", lambda e: 'break')
        self.tree.bind("<<TreeviewOpen>>", self.setSkipSelection)
        self.tree.bind("<<TreeviewClose>>", self.setSkipSelection)

        self.frame_industryBtns = tkr.Frame(self)
        self.btn_expandAll = tkr.Button(self.frame_industryBtns, text="Expand All", command=self.btn_expandAll)
        self.btn_collapseAll = tkr.Button(self.frame_industryBtns, text="Collapse All", command=self.btn_collapseAll)
        self.btn_selectAll = tkr.Button(self.frame_industryBtns, text="Select All", command=self.btn_cmd_selectAll)
        self.btn_removeAll = tkr.Button(self.frame_industryBtns, text="Remove All", command=self.btn_cmd_deselectAll)
        self.btn_OK = tkr.Button(self.frame_industryBtns, text="OK", command=self.btn_cmd_OK)

        for k, v in res.items():
            self.tree.insert("", 'end', iid=k, text=str(k) + ' ' + v['description'])
            if v.get('children', None):
                self.addNode(k,v['children'], k)

        self.vScroll.pack(side=tkr.RIGHT, fill='y')
        # self.hScroll.pack(side=tkr.BOTTOM, fill='x')
        self.tree.pack(expand=True, side=tkr.TOP, fill='both')
        self.frame_tree.pack(expand=True, side=tkr.TOP, fill=tkr.BOTH)
        self.btn_OK.pack(side=tkr.RIGHT, fill=tkr.X)
        self.btn_removeAll.pack(side=tkr.RIGHT, fill=tkr.X)
        self.btn_selectAll.pack(side=tkr.RIGHT, fill=tkr.X)
        self.btn_collapseAll.pack(side=tkr.RIGHT, fill=tkr.X)
        self.btn_expandAll.pack(side=tkr.RIGHT, fill=tkr.X)
        self.frame_industryBtns.pack(side='right', fill='x', expand=True)

        global industryCodesSelection
        if industryCodesSelection:
            self.tree.selection_add(industryCodesSelection)

        self.grab_set()
        self.wait_window()  

    def closeAction(self):
        self.destroy()
        self.selectionButton.config(state='normal')
        return

    def select_children(self, parent, method):
        method(parent)
        for child in self.tree.get_children(parent):
            self.select_children(child, method)
        return
                

    def select(self, event=None):
        if self.skipSelection:
            self.skipSelection = False
        else:
            method = self.tree.selection_remove if self.tree.focus() in self.tree.selection() else self.tree.selection_add
            self.select_children(self.tree.focus(), method)
        return

    def setSkipSelection(self, event=None):
        self.skipSelection = True
        return

    def addNode(self, k,v, parent=""):
        for i, j in v.items():
            self.tree.insert(parent, 'end', iid=i, text=str(i) + ' ' + j['description'])
            if j.get('children', None):
                self.addNode(i, j['children'], i)
        return

    def setTreeItemOpen(self, node, open=True):
        if node:
            self.tree.item(node, open=open)
        for childNode in self.tree.get_children(node):
            self.setTreeItemOpen(childNode, open)
        return

    def btn_expandAll(self):
        self.setTreeItemOpen("",open=True)
        return

    def btn_collapseAll(self):
        self.setTreeItemOpen("",open=False)
        return

    def btn_cmd_selectAll(self):
        roots = self.tree.get_children()
        for item in roots:
            self.select_children(item, self.tree.selection_add)
        return

    def btn_cmd_deselectAll(self):
        global industryCodesSelection
        roots = self.tree.get_children()
        for item in roots:
            self.select_children(item, self.tree.selection_remove)
        industryCodesSelection = self.tree.selection()
        return

    def btn_cmd_OK(self):
        global industryCodesSelection
        if self.tree.selection():
            industryCodesSelection = self.tree.selection()
        self.closeAction()
        return

class storeInXbrlDBSettings(tkr.Toplevel):
    def __init__(self, searchView, cntlr, master, selectionButton: tkr.Button, selectedItems, **kw):
        super().__init__(master, **kw)
        self.searchView = searchView
        self.modelXbrl = searchView.modelXbrl
        self.options = cntlr.config.get("rssQueryResultsActions", {})
        self.cntlr = cntlr
        self.selectionIds = selectedItems
        self.selectedRssItems = [self.modelXbrl.modelObject(x) for x in selectedItems]
        self.selectionButton = selectionButton
        self.selectionButton.config(state='disabled')
        self.wm_title(_('Store In Xbrl DB'))
        self.protocol('WM_DELETE_WINDOW', self.closeAction)

        #vars
        self.runFormulaeOptions = dict()
        
        y = 0
        selectionNote = tkr.Label(self, text='{} rss items selected'.format(len(selectedItems)), name='selectionNote',width=8, anchor='w')
        selectionNote.grid(row=y, column=0, sticky=tkr.EW, pady=0, padx=3)

        y+=1        
        storeInXbrlDBFrame = tkr.LabelFrame(self, text='Store in XBRL DB')
        storeInXbrlDBFrame.grid(row=y, column=0, sticky=tkr.EW, pady=0, padx=3) 

        label(storeInXbrlDBFrame, 1, 0, text=_("DB Connection:"))
        self.storeInXbrlDBEntry = gridCell(storeInXbrlDBFrame, 2 , 0, value=self.options.get('storeInXbrlDBparams',''))
        self.storeInXbrlDBEntry.grid(pady=3, padx=3)
        ToolTip(self.storeInXbrlDBEntry, text=_("Enter an XBRL Database (Postgres) connection string.  "
                                           "E.g., host,port,user,password,db[,timeout].  "), wraplength=240)
        self.xbrlDBImage = tkr.PhotoImage(file=os.path.join(self.cntlr.imagesDir, "toolbarOpenDatabase.gif"))
        self.btn_storeInXbrlDBparams = tkr.Button(storeInXbrlDBFrame, image=self.xbrlDBImage, command=self.btn_cmd_storeInXbrlDBparams)
        self.btn_storeInXbrlDBparams.grid(row=0, column=3, sticky=tkr.EW)

        self.btn_store_action = tkr.Button(storeInXbrlDBFrame, text = _('Store In XBRL DB'), command=self.btn_cmd_store_action)
        self.btn_store_action.grid(row=1, column=0, columnspan=4, sticky=tkr.EW)

        makeWeight(storeInXbrlDBFrame,rows=False)
        self.grab_set() # block changing for treeview
        self.wait_window() 

    def btn_cmd_store_action(self):
        if self.storeInXbrlDBEntry.value:
            items = sorted(self.selectedRssItems, key=lambda x:x.pubDate, reverse=True)
            try: # let make sure we have a connection to the db before we get excited
                _dbCon = [x.strip() if x.strip() else None for x in self.storeInXbrlDBEntry.value.split(',')]
                conFunc = _dbTypes.get(_dbCon[6], None)
                _conn = conFunc(self.modelXbrl, _dbCon[2], _dbCon[3],_dbCon[0], _dbCon[1], _dbCon[4], _dbCon[5], dbProduct.get(_dbCon[6], None))
                _conn.close()
            except Exception as e:
                self.cntlr.addToLog(_('Could not connect to db\n{}').format(str(e)), messageCode='rssDB.Error', level=logging.ERROR)
                return
            t1 = threading.Thread(target=storeInToXbrlDB, args=(self.cntlr, items, self.storeInXbrlDBEntry.value, self.selectionButton), daemon=True)
            t1.start()
        else:
            messagebox.showerror(_("RSS DB Error"), _("No Connection paramaters given"), parent=self.cntlr.parent)
            return
        self.closeAction()
        return

    def btn_cmd_storeInXbrlDBparams(self):
        # from xbrlDB/DialogRssWatchExtender.py
        from arelle.DialogUserPassword import askDatabase
        previousPrams = None
        if self.options.get('storeInXbrlDB', None):
            previousPrams = self.options.get('storeInXbrlDB', '').split(',')
        # (user, password, host, port, database)
        db = askDatabase(self, previousPrams)
        self.grab_set()
        if db:
            dbConnectionString = ','.join(db)
            # self.options["storeInXbrlDB"] = dbConnectionString 
            self.storeInXbrlDBEntry.setValue(dbConnectionString)
        else:  # deleted
            self.options.pop("storeInXbrlDB", "")  # remove entry
        return

    def closeAction(self, save=True):
        if self.storeInXbrlDBEntry.value:
            self.options['storeInXbrlDB'] = self.storeInXbrlDBEntry.value
            self.cntlr.config['rssQueryResultsActions'] = self.options
        if save:
            self.cntlr.saveConfig()
        self.selectionButton.config(state='normal')
        self.destroy()
        # self.cntlr.addToLog(self.options)
        return

class ViewRssDBQuery(ViewWinRssFeed.ViewRssFeed):
    '''based on arelle.ViewWinRssFeed.ViewRssFeed'''
    global con_dependent_ui
    def __init__(self, modelXbrl, tabWin, title, queryParams=None, q=None):
        super().__init__(modelXbrl, tabWin)
        self.queryParams = queryParams
        self.multiprocessQueue = None #q if q else queue.Queue()
        # tabWin.tab(len(tabWin.children)-1, text=title)
        con = self.modelXbrl.modelManager.cntlr.dbConnection
        self.rssDBFrame = con.rssDBFrame
        self.conKey = {k:v for k,v in con.conParams.items()}
        self.selectedRssItems = None
        self.treeView.config(selectmode="none")
        # self.treeView.grid(sticky=tkr.EW)
        optionsFrame = tkr.Frame(self.viewFrame)
        optionsFrame.grid(row=2, column=0, sticky=tkr.NSEW)
        self.frame_Btns = tkr.LabelFrame(optionsFrame, text=_("Selection and Save Options"))
        self.btn_selectAll = tkr.Button(self.frame_Btns, text="Select All", command=self.btn_cmd_selectAll)
        self.btn_selectAll.grid(row=0, column=0, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_deslectAll = tkr.Button(self.frame_Btns, text="Deselect All", command=self.btn_cmd_deselectAll)
        self.btn_deslectAll.grid(row=0, column=1, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_refresh = tkr.Button(self.frame_Btns, text="Refresh", command=self.btn_cmd_refresh)
        ToolTip(self.btn_refresh, text=_("Refreshes query result, useful after updating db"), wraplength=360)
        self.btn_refresh.grid(row=0, column=2, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_removeSelected = tkr.Button(self.frame_Btns, text="Remove Selected", command=self.btn_cmd_removeSelected)
        ToolTip(self.btn_removeSelected, text=_("Remove selected items from result"), wraplength=360)
        self.btn_removeSelected.grid(row=1, column=0, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_keepSelected = tkr.Button(self.frame_Btns, text="Keep Selected", command=self.btn_cmd_keepSelected)
        ToolTip(self.btn_keepSelected, text=_("Keeps selected items and removes all other items from result"), wraplength=360)
        self.btn_keepSelected.grid(row=1, column=1, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_saveResult = tkr.Button(self.frame_Btns, text="Save As", command=self.btn_cmd_saveAs)
        ToolTip(self.btn_saveResult, text=_("Saves the result as an xml rss feed document that can be parsed by arelle as rss feed"), wraplength=360)
        self.btn_saveResult.grid(row=1, column=2, padx=3, pady=3, sticky=tkr.NSEW)
        
        self.frame_actions = tkr.LabelFrame(optionsFrame, text=_("Do actions on selection"))
        self.btn_renderEdgarReports = tkr.Button(self.frame_actions, text="Render Edgar Reports", command=self.renderEdgarReports)
        ToolTip(self.btn_renderEdgarReports, text=_("Renders Edgar reports for the selected items and opens viewer, a folder needs to be selected to store rendered reports"), wraplength=360)
        self.btn_renderEdgarReports.grid(row=0, column=0, rowspan=2, padx=3, pady=3, sticky=tkr.NSEW) 
        self.btn_formula = tkr.Button(self.frame_actions, text="Run XBRL Formula", command=self.btn_cmd_formula)
        ToolTip(self.btn_formula, text=_("Opens rssDB formulae options to run on selected items - this is different from arelle 'formulae options'"), wraplength=360)        
        self.btn_formula.grid(row=0, column=1, rowspan=2, padx=3, pady=3, sticky=tkr.NSEW)
        self.btn_storeInXbrlDB = tkr.Button(self.frame_actions, text="Store In XBRL DB", command=self.btn_cmd_storeInXBRLDB)
        ToolTip(self.btn_storeInXbrlDB, text=_("Store selected filings into XBRL db"), wraplength=360)
        self.btn_storeInXbrlDB.grid(row=0, column=2, rowspan=2, padx=3, pady=3, sticky=tkr.NSEW)

        self.frame_Btns.grid(row=0, column=0, sticky=tkr.NSEW, padx=3, pady=3)
        self.frame_actions.grid(row=0, column=1, sticky=tkr.NSEW, padx=3, pady=3)
        makeWeight(optionsFrame)
        makeWeight(self.frame_Btns)
        makeWeight(self.frame_actions)

    def close(self):
        del self.viewFrame.view
        if self.modelXbrl:
            self.tabWin.forget(self.viewFrame)
            self.modelXbrl.views.remove(self)
            self.modelXbrl.modelManager.close()
            del self.modelXbrl
            self.view = None
        con_dependent_ui.remove(self)
        return

    def btn_cmd_storeInXBRLDB(self):
        if len(self.treeView.selection()):
            storeInXbrlDBSettings(self, self.modelXbrl.modelManager.cntlr, self.modelXbrl.modelManager.cntlr.parent, self.btn_storeInXbrlDB, self.treeView.selection())
        else:
            messagebox.showerror(_("RSS DB Info"), _("There are no Rss Items selected"), parent=self.modelXbrl.modelManager.cntlr.parent)
            return
        return

    def btn_cmd_formula(self):
        # make sure there is a selection
        if not len(self.treeView.selection()):
            messagebox.showerror(_("RSS DB Info"), _("There are no Rss Items selected"), parent=self.modelXbrl.modelManager.cntlr.parent)
            return
        # make sure we are using same connection that produced the search result
        if self.conKey == getattr(self.modelXbrl.modelManager.cntlr.dbConnection, 'conParams', None):
            formulaDialog = runFormulaDialog(self, self.modelXbrl.modelManager.cntlr, self.modelXbrl.modelManager.cntlr.parent, self.btn_formula,includeRun=True)
        else:
            messagebox.showerror(_("RSS DB Error"), _("There is no active DB connection or current connection is different "
                                                        "from the connection that produced the search results"), parent=self.modelXbrl.modelManager.cntlr.parent)
            return
        return

    def rssDBtreeviewSelect(self, *args):
        if self.blockSelectEvent == 0 and self.blockViewModelObject == 0:
            self.blockViewModelObject += 1
            method = self.treeView.selection_remove if self.treeView.focus() in self.treeView.selection() else self.treeView.selection_add
            currSelection = self.treeView.focus()
            method(currSelection)
            self.modelXbrl.viewModelObject(currSelection)
            self.blockViewModelObject -= 1
        return

    def viewRssFeed(self, modelDocument, parentNode):
        self.id = 1
        for rssItem in modelDocument.rssItems:
            rssItem.results = []
            isInline = rssItem.find('isInlineXBRL').text
            node = self.treeView.insert(parentNode, "end", rssItem.objectId(),
                                        text=(rssItem.cikNumber or ''),
                                        tags=("odd" if self.id & 1 else "even",))
            self.treeView.set(node, "form", rssItem.formType)
            self.treeView.set(node, "inlineXBRL", 'YES' if isInline == 'true' else 'NO')
            self.treeView.set(node, "filingDate", rssItem.filingDate)
            self.treeView.set(node, "companyName", (rssItem.companyName or ''))
            self.treeView.set(node, "sic", rssItem.assignedSic if rssItem.assignedSic else '--')
            self.treeView.set(node, "industryName", flatIndustry.get(rssItem.assignedSic, 'Not Assigned'))
            self.treeView.set(node, "status", rssItem.status)
            self.treeView.set(node, "period", rssItem.period)
            self.treeView.set(node, "fiscalYrEnd", rssItem.fiscalYearEnd)
            self.treeView.set(node, "results", rssItem.results[0] if len(rssItem.results)>0 and isinstance(rssItem.results, list) else \
                                             " ".join(str(result) for result in (rssItem.results or [])) +
                                                ((" " + str(rssItem.assertions)) if rssItem.assertions else ""))
            self.id += 1
        else:
            pass
        return

    def btn_cmd_selectAll(self):
        roots = self.treeView.get_children()
        for item in roots:
            self.treeView.selection_add(item)
        return

    def btn_cmd_deselectAll(self):
        roots = self.treeView.get_children()
        for item in roots:
            self.treeView.selection_remove(item)
        return
    
    def btn_cmd_removeSelected(self):
        selected_items = self.treeView.selection()
        if selected_items:
            for item in selected_items:
                self.treeView.delete(item)
    
    def btn_cmd_keepSelected(self):
        selected_items = self.treeView.selection()
        if selected_items:
            roots = self.treeView.get_children()
            not_selected = list(set(roots)-set(selected_items))
            for item in not_selected:
                self.treeView.delete(item)
    
    def renderEdgarReports(self, saveFolder=None):
        from arellepy.LocalViewerStandalone import startEdgarViewer
        global getQueue_render, MAKEDOTS_RSSDBPANEL
        ids = self.treeView.selection()
        cntlr = self.modelXbrl.modelManager.cntlr
        appDir = os.path.dirname(cntlr.configDir)
        requiredPlugins = ['validate/EFM','EdgarRenderer','transforms/SEC'] 
        pluginsDir = []
        for p in requiredPlugins:
            chkPlugin = os.path.isdir(os.path.join(appDir, 'plugin', p))
            if not chkPlugin:
                if cntlr.hasGui:
                    getP = messagebox.askyesno(title='Plugin Info',
                                                message='This feature requires {} Plugin and it is not available in the default location\nChoose the location of {} plugin?'.format(p,p),
                                                parent=cntlr.parent)
                    if getP:
                        pDir = filedialog.askdirectory(title=_("Select {} Directory".format(p)), parent=cntlr.parent)
                        pluginsDir.append(pDir)
                    else:
                        messagebox.showinfo(title='Plugin info', message='Aborting Renderer...', parent=cntlr.parent)
                        return
                    if not os.path.isdir(pDir):
                        messagebox.showinfo(
                            title='RSS DB info', message='Could not find {} plugin, please enter valid location for the plugin\nAborting Renderer...'.format(p), parent=cntlr.parent)
                        return
                else:
                    cntlr.addToLog(_("This feature requires {} plugin, please enter a valid value for edgarDir. Aborting...".format(p)),
                                messageCode="EdgarViewer", file="",  level=logging.INFO)
                    return
            else:
                pluginsDir.append(p)
        if not saveFolder:
            saveFolder = filedialog.askdirectory(title=_("Select a directory to save generated Edgar Filings"))
        if saveFolder:
            getQueue_render = True
            _key = 'render'
            MAKEDOTS_RSSDBPANEL[_key] = True
            t1 = threading.Thread(target=self.background_renderEdgarReports, args=(saveFolder, ids, pluginsDir,_key), daemon=True)
            t2 = threading.Thread(target=dotted, args=(self.modelXbrl.modelManager.cntlr , _key,'Rendering',), daemon=True)
            t2.start()
            t1.start()
            startEdgarViewer(self.modelXbrl.modelManager.cntlr, edgarDir=os.path.join(appDir, 'plugin', pluginsDir[1]))
        else:
            messagebox.showinfo('RSS DB Info', 'No directory was provided to save output, Aborted', parent=self.modelXbrl.modelManager.cntlr.parent)
            return
 
    def _saveAs_helper(self):
        from .Constants import pathToTemplates
        cntlr = self.modelXbrl.modelManager.cntlr
        feedF = filedialog.asksaveasfilename(title=_('Save query results to file'), filetypes=[(("XML"), ".xml .XML")])
        if feedF is None:
            messagebox.showinfo(title=_("RSS DB INFO"), message=_("No file name provided, aborting!"), icon='warning')
            return

        try:
            cntlr.showStatus(_('Writing query result to file'))
            timeNow = datetime.datetime.now(tz.tzlocal()).strftime("%a, %d %b %Y %H:%M:%S %Z")
            rssTemplate = os.path.join(pathToTemplates, 'rssFeedtmplt.xml') 
            with open(rssTemplate, 'r') as f:
                rssTmplt = f.read().format(a=timeNow)

            rssDoc = etree.fromstring(rssTmplt)
            rssChannel = rssDoc.find('channel')
            ids= self.treeView.get_children()
            pubDateRssItems = []
            _items = [self.modelXbrl.modelObject(x) for x in ids]
            for rssItem in _items:
                pubDateRssItems.append((rssItem.pubDate, rssItem))
            sortedItems = sorted(pubDateRssItems, key=lambda x:x[0], reverse=True)
            sortedRssItems = [x[1] for x in sortedItems]

            for itemEl in sortedRssItems:
                _stat = etree.Element('status')
                _stat.text = getattr(itemEl, 'status', '')
                _reuslts = etree.Element('results')
                _reuslts.text = str(getattr(itemEl, 'results', ''))
                itemEl.append(_stat)
                itemEl.append(_reuslts)
                rssChannel.append(itemEl)
            feedString = etree.tostring(rssDoc, pretty_print=True)
            with open(feedF, 'wb') as sf:
                sf.write(feedString)
            cntlr.addToLog(_("Saved result to {}").format(feedF), messageCode="RssDB.Info", file=f, level=logging.INFO)
        except Exception as e:
            cntlr.addToLog(_("Error while saving query to file:\n{}").format(str(e)), messageCode="RssDB.Error", file=f, level=logging.ERROR)
        
        return
    
    def btn_cmd_saveAs(self):
        t = threading.Thread(target=self._saveAs_helper, daemon=True)
        t.start()
        return 

    def btn_cmd_refresh(self):
        qParams = self.queryParams
        self.rssDBFrame.backgroundSearchDB(qParams)
        return


    def _backgroundGetQ(self):
        global getQueue_render
        cntlr = self.modelXbrl.modelManager.cntlr
        callback = {'showStatus': cntlr.showStatus, 'addToLog': cntlr.addToLog}
        while getQueue_render:
            if not self.multiprocessQueue.empty():
                callbackName, args = self.multiprocessQueue.get()
                cntlr.waitForUiThreadQueue()
                cntlr.uiThreadQueue.put((callback[callbackName],args))
        return
           
    def background_renderEdgarReports(self, saveToFolder, ids, pluginsDirs, _key):
        from arellepy.CntlrPy import renderEdgarReports
        global getQueue_render, MAKEDOTS_RSSDBPANEL
        startTime = time.perf_counter()
        self.btn_renderEdgarReports.config(state='disabled')
        pubDateRssItems = []
        _items = [self.modelXbrl.modelObject(x) for x in ids]
        n = 0
        for rssItem in _items:
            pubDateRssItems.append((rssItem.pubDate,rssItem.objectId()))
        for pubDate, rssItemObjectId in sorted(pubDateRssItems, key=lambda x: x[0], reverse=True):
            rssItem = self.modelXbrl.modelObject(rssItemObjectId)
            if not isinstance(rssItem.results, list):
                rssItem.results = []
            self.modelXbrl.modelManager.viewModelObject(self.modelXbrl, rssItem.objectId())
            # get information from item
            res = []
            reportFolder = None
            plugins = pluginsDirs
            statusMsg = ''
            try:
                rssItem.status = 'Render Edgar Reports'
                self.modelXbrl.modelManager.viewModelObject(self.modelXbrl, rssItem.objectId())
                _start = time.perf_counter()
                reportFolder, errors = renderEdgarReports(rssItem, saveToFolder, plugins, self.multiprocessQueue)                   
                _end = time.perf_counter()
                if len(errors):
                    rssItem.results.extend(errors)
                    self.modelXbrl.modelManager.viewModelObject(self.modelXbrl, rssItem.objectId())
                    statusMsg = _('Errors {}  while rendering form {} for {} in {} secs').format(','.join(errors), rssItem.formType, rssItem.companyName, round(_end-_start,3))
                else:
                    res.append(reportFolder)
                    rssItem.results = [reportFolder]
                    self.modelXbrl.modelManager.viewModelObject(self.modelXbrl, rssItem.objectId())
                    statusMsg = _('Done rendering form {} for {} in {} secs').format(rssItem.formType, rssItem.companyName, round(_end-_start,3))
                self.modelXbrl.modelManager.cntlr.addToLog(statusMsg, messageCode="RssDB.Info", file="",  level=logging.INFO)
                n +=1
            except Exception as e:
                getQueue_render = False
                # self.modelXbrl.modelManager.cntlr.addToLog('Error while processing {}'.format(str(rssItem)), messageCode="RssDB.Error", file="",  level=logging.ERROR)
                tkr.messagebox.showerror(_("RSS DB Render Edgar error(s)"), '{}\n{}'.format(str(e), traceback.format_tb(sys.exc_info()[2])), parent=self.modelXbrl.modelManager.cntlr.parent)

        getQueue_render = False
        MAKEDOTS_RSSDBPANEL[_key] = False
        endTime = time.perf_counter()
        self.modelXbrl.modelManager.cntlr.addToLog(_('Done with Rendering {} reports in {} secs').format(n,round(endTime-startTime,3)), messageCode="RssDB.Info", file="",  level=logging.INFO)
        self.btn_renderEdgarReports.config(state='normal')
        self.modelXbrl.modelManager.cntlr.waitForUiThreadQueue()
        self.modelXbrl.modelManager.cntlr.uiThreadQueue.put((self.btn_cmd_deselectAll,[]))
        return

def rssDBviewRssFeed(modelXbrl, tabWin, title, queryParams, q=None):
    '''based on arelle.ViewWinRssFeed.viewRssFeed'''
    global con_dependent_ui
    view = ViewRssDBQuery(modelXbrl, tabWin, title, queryParams, q)
    tabWin.tab(len(tabWin.tabs())-1, text=title)
    view.title = lambda: title
    modelXbrl.modelManager.showStatus(_("viewing RSS DB Search Results"))
    view.treeView["columns"] = ("companyName", "form", 'inlineXBRL', "sic", "industryName" , "filingDate", "period", "fiscalYrEnd", "status", "results")
    view.treeView.column("#0", width=70, anchor="w")
    view.treeView.heading("#0", text="CIK")
    view.treeView.column("form", width=30, anchor="w")
    view.treeView.heading("form", text="Form")
    view.treeView.column("inlineXBRL", width=30, anchor="w")
    view.treeView.heading("inlineXBRL", text="Inline")
    view.treeView.column("filingDate", width=60, anchor="w")
    view.treeView.heading("filingDate", text="Filing Date")
    view.treeView.column("companyName", width=220, anchor="w")
    view.treeView.heading("companyName", text="Company Name")
    view.treeView.column("sic", width=30, anchor="w")
    view.treeView.heading("sic", text="SIC")
    view.treeView.column("industryName", width=240, anchor="w")
    view.treeView.heading("industryName", text="Industry Name")
    view.treeView.column("status", width=70, anchor="w")
    view.treeView.heading("status", text="Status")
    view.treeView.column("period", width=50, anchor="w")
    view.treeView.heading("period", text="Period")
    view.treeView.column("fiscalYrEnd", width=25, anchor="w")
    view.treeView.heading("fiscalYrEnd", text="Yr End")
    view.treeView.column("results", width=50, anchor="w")
    view.treeView.heading("results",  text="Results")
    view.view()
    view.blockSelectEvent = 1
    view.blockViewModelObject = 0
    # view.treeView.bind("<<TreeviewSelect>>", view.treeviewSelect, '+')
    view.treeView.bind("<ButtonRelease-1>", view.rssDBtreeviewSelect)
    view.treeView.bind("<Enter>", view.treeviewEnter, '+')
    view.treeView.bind("<Leave>", view.treeviewLeave, '+')
    
    # menu
    # intercept menu click before pops up to set the viewable RSS item htm URLs
    view.treeView.bind( view.modelXbrl.modelManager.cntlr.contextMenuClick, view.setMenuHtmURLs, '+' )
    cntxMenu = view.contextMenu()
    view.setMenuHtmURLs()
    # rssWatchMenu = tkr.Menu(view.viewFrame, tearoff=0)
    # rssWatchMenu.add_command(label=_("Options..."), underline=0, command=lambda: modelXbrl.modelManager.cntlr.rssWatchOptionsDialog())
    # rssWatchMenu.add_command(label=_("Start"), underline=0, command=lambda: modelXbrl.modelManager.cntlr.rssWatchControl(start=True))
    # rssWatchMenu.add_command(label=_("Stop"), underline=0, command=lambda: modelXbrl.modelManager.cntlr.rssWatchControl(stop=True))
    # cntxMenu.add_cascade(label=_("RSS Watch"), menu=rssWatchMenu, underline=0)
    view.menuAddClipboard()
    con_dependent_ui.append(view)

class rssDBUpdateSettings(tkr.LabelFrame):
    def __init__(self, master=None, **kw):
        self.accepted = False
        self.vals = dict()
        self.params = ['dateFrom', 'dateTo', 'maxWorkers', 'timeOut',
                     'retries', 'getXML', 'includeLatest', 'reloadCache',
                     'updateTickers', 'getFilers', 'updateExisting', 'refreshAll', 
                     'setAutoUpdate', 'waitFor', 'duration']
        self.fromDateVar = tkr.StringVar()
        self.fromDateVar.set('')
        self.toDateVar = tkr.StringVar()
        self.toDateVar.set('')
        self.maxWorkersVar = tkr.IntVar()
        self.maxWorkersVar.set(os.cpu_count()/2)
        self.timeOutVar = tkr.IntVar()
        self.timeOutVar.set(3)
        self.retriesVar = tkr.IntVar()
        self.retriesVar.set(3)

        self.waitForVar = tkr.IntVar()
        self.waitForVar.set(10)
        self.daysVar = tkr.IntVar()
        self.daysVar.set(0)
        self.hoursVar = tkr.IntVar()
        self.hoursVar.set(1)
        self.minutesVar = tkr.IntVar()
        self.minutesVar.set(0)

        super().__init__(master=master, **kw)
        y = 0
        firstGroupFrame = tkr.LabelFrame(self, text = 'Data Settings' , padx=5, pady=5)
        firstGroupFrame.grid(column=0, row=y, padx=3, pady=3, sticky=tkr.EW)
        # firstGroupFrame.conf()
        y_1 = 0
        dateRangeLabel = tkr.Label(firstGroupFrame, text='Date Range -->')
        dateRangeLabel.grid(row=y_1, column=0, sticky=tkr.W, pady=3, padx=3)
        ToolTip(dateRangeLabel, _('Date Range of feeds to retrive for this update, safe -- feeds info existing in the db are not duplicated'))
        fromDateLabel = tkr.Label(firstGroupFrame, text='From:', anchor="e")
        fromDateLabel.grid(row=y_1, column=1, sticky=tkr.E, pady=3, padx=3)
        self.fromDateEntry = tkr.Entry(firstGroupFrame,  textvariable=self.fromDateVar, name='fromDateEntry') # width=10,
        self.fromDateEntry.grid(row=y_1, column=2, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.fromDateEntry, _('Date in the format: yyyy-mm-dd, if left empty, gets from the first available feed'))
        toDateLabel = tkr.Label(firstGroupFrame, text='To:')
        toDateLabel.grid(row=y_1, column=4, sticky=tkr.E, pady=3, padx=3)
        self.toDateEntry = tkr.Entry(firstGroupFrame,  textvariable=self.toDateVar, name='toDateEntry') # width=10,
        self.toDateEntry.grid(row=y_1, column=5, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.toDateEntry, _('Date in the format: yyyy-mm-dd, if left empty, gets up to the last available feed'))
        
        y_1 += 1
        self.getXML_cb =  checkbox(firstGroupFrame, 0, y_1, columnspan=2, text=_("Store XML RSS Items"))
        self.getXML_cb._name ='getRssItems'
        self.getXML_cb.valueVar.set(0)
        self.getXML_cb.grid(padx=3)
        ToolTip(self.getXML_cb, text=_(("Whether to store RSS Items as XML, Makes it easier to rebuild \n"
                                         "RSS Feed Like document to be processed by Arelle when DB is queried \n"
                                         "(not recommended, takes up too much storage)")), wraplength=360)

        self.getLatest_cb =  checkbox(firstGroupFrame, 2, y_1, columnspan=2, text=_("Get Latest Filings"))
        self.getLatest_cb._name ='getLatest'
        self.getLatest_cb.valueVar.set(1)
        self.getLatest_cb.grid(padx=3)
        ToolTip(self.getLatest_cb, text=_("Whether to get latest filings not included yet in the monthly feed file"), wraplength=360)
        
        self.reloadCache_cb =  checkbox(firstGroupFrame, 4, y_1, columnspan=2, text=_("Reload Cached Files"))
        self.reloadCache_cb._name ='reloadCache'
        self.reloadCache_cb.valueVar.set(0)
        self.reloadCache_cb.grid(padx=3)
        ToolTip(self.reloadCache_cb, text=_("Whether to reload cached feeds files before parsing -- usually not recommended"), wraplength=360) 
        y_1 +=1

        maxWorkersLabel = tkr.Label(firstGroupFrame, text='Max Processes:')
        maxWorkersLabel.grid(row=y_1, column=0, sticky=tkr.E, pady=3, padx=3)
        self.maxWorkersEntry = tkr.Entry(firstGroupFrame, width=3, textvariable=self.maxWorkersVar, name='maxWorkersEntry')
        self.maxWorkersEntry.grid(row=y_1, column=1, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.maxWorkersEntry, _('Max number of processes to be used in the background, defaults to half of the total available cores if left empty -- use with care'))

        self.updateTickerMapping_cb =  checkbox(firstGroupFrame, 2, y_1, columnspan=6, text=_("Update Ticker CIK mapping"))
        self.updateTickerMapping_cb._name ='updateTickerMapping'
        self.updateTickerMapping_cb.valueVar.set(1)
        self.updateTickerMapping_cb.grid(padx=10)
        ToolTip(self.updateTickerMapping_cb, text=_("Whether to ticker CIK mapping from SEC web site -- recommended"), wraplength=360) 
        
        y +=1
        y_2 = 0
        secondGroupFrame = tkr.LabelFrame(self, text = 'Filers Data Settings' , padx=5, pady=5)
        secondGroupFrame.grid(column=0, row=y, padx=3, pady=3, sticky=tkr.EW)

        self.getFilersInfo_cb =  checkbox(secondGroupFrame, 0, y_2, text=_("Get Filers Info"))
        self.getFilersInfo_cb._name ='getFilersInfo'
        self.getFilersInfo_cb.valueVar.set(1)
        self.getFilersInfo_cb.grid(padx=3)
        ToolTip(self.getFilersInfo_cb, text=_("Whether to get Filers Info -- takes some time in initial load"), wraplength=360) 

        self.updateFilersInfo_cb =  checkbox(secondGroupFrame, 2, y_2, text=_("Update Filers Info"))
        self.updateFilersInfo_cb._name ='updateFilersInfo'
        self.updateFilersInfo_cb.valueVar.set(1)
        self.updateFilersInfo_cb.grid(padx=3)
        ToolTip(self.updateFilersInfo_cb, text=_("Whether to update existing Filers Info, tries to locate filers with changes in their information and update them."), wraplength=360) 

        self.refreshFilersInfo_cb =  checkbox(secondGroupFrame, 4, y_2, text=_("Refresh Filers Info"))
        self.refreshFilersInfo_cb._name ='refreshFilersInfo'
        self.refreshFilersInfo_cb.valueVar.set(0)
        self.refreshFilersInfo_cb.grid(padx=3)
        ToolTip(self.refreshFilersInfo_cb, text=_("Retrives All filers information again from SEC website -- useful when lot of changes in filers information is noticed"), wraplength=360) 

        y_2 +=1
        timeOutLabel = tkr.Label(secondGroupFrame, text='Request timeout:')
        timeOutLabel.grid(row=y_2, column=0, sticky=tkr.E, pady=3, padx=3)
        self.timeOutEntry = tkr.Entry(secondGroupFrame, width=5, textvariable=self.timeOutVar, name='timeOutEntry')
        self.timeOutEntry.grid(row=y_2, column=1, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.timeOutEntry, _('Time out for retriving a filer\'s information from SEC website. Defaults to 3 seconds'))
        retriesLabel = tkr.Label(secondGroupFrame, text='Retries:')
        retriesLabel.grid(row=y_2, column=2, sticky=tkr.E, pady=3, padx=3)
        self.retriesEntry = tkr.Entry(secondGroupFrame, width=5, textvariable=self.retriesVar, name='retriesEntry')
        self.retriesEntry.grid(row=y_2, column=3, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.retriesEntry, _('Number of times to try getting filer info, defaults to 3 retries'))       
        y +=1

        thirdGroupFrame = tkr.LabelFrame(self, text = _('Auto-update Settings') , padx=5, pady=5)
        thirdGroupFrame.grid(column=0, row=y, padx=3, pady=3, sticky=tkr.EW)
        self.setAutoUpdate_cb =  checkbox(thirdGroupFrame, 0, 0, text=_("Set auto-update ON"))
        self.setAutoUpdate_cb._name ='setAutoUpdate'
        self.setAutoUpdate_cb.valueVar.set(0)
        self.setAutoUpdate_cb.grid(padx=3)
        ToolTip(self.setAutoUpdate_cb, _('Runs a separate thread to automatically update DB as per the entered options for the specified duration'), wraplength=360)

        waitForLabel = tkr.Label(thirdGroupFrame, text=_('Check for updates every (in minutes - minimum is 10)'))
        waitForLabel.grid(row=0, column=2, sticky=tkr.E, pady=3, padx=3)
        self.waitForEntry = tkr.Entry(thirdGroupFrame, width=3, textvariable=self.waitForVar, name='waitForLabel')
        self.waitForEntry.grid(row=0, column=3, sticky=tkr.E, padx=3,pady=3)
        ToolTip(self.waitForEntry, _('Time to wait before rechecking for updates, minimum 10 mins, SEC updates the rss feed every 10 mins'), wraplength=360)

        durationFrame = tkr.Frame(thirdGroupFrame)
        durationFrame.grid(row=1, column=0, sticky=tkr.EW, columnspan=5)
        durationLabel = tkr.Label(durationFrame, text=_('For a duration of:'))
        durationLabel.grid(row=0, column=0, sticky=tkr.E, pady=3, padx=3)

        self.daysEntry = tkr.Entry(durationFrame, width=3, textvariable=self.daysVar, name='daysEntry')
        self.daysEntry.grid(row=0, column=1, sticky=tkr.E, pady=3, padx=(3,0))
        self.daysLabel = tkr.Label(durationFrame, text=_('day(s)'), anchor='w')
        self.daysLabel.grid(row=0, column=2, sticky=tkr.W, pady=3, padx=(0,3))

        self.hoursEntry = tkr.Entry(durationFrame, width=3, textvariable=self.hoursVar, name='hoursEntry')
        self.hoursEntry.grid(row=0, column=3, sticky=tkr.E, pady=3, padx=(3,0))
        self.hoursLabel = tkr.Label(durationFrame, text=_('hour(s)'), anchor='w')
        self.hoursLabel.grid(row=0, column=4, sticky=tkr.W, pady=3, padx=(0,3))

        self.minutesEntry = tkr.Entry(durationFrame, width=3, textvariable=self.minutesVar, name='minutesEntry')
        self.minutesEntry.grid(row=0, column=5, sticky=tkr.E, pady=3, padx=(3,0))
        self.minutesLabel = tkr.Label(durationFrame, text=_('minute(s)'), anchor='w')
        self.minutesLabel.grid(row=0, column=6, sticky=tkr.W, pady=3, padx=(0,3))

        self.btn_stopAutoUpdate  = tkr.Button(durationFrame, text=_('Stop Auto-update'), command= self._stopAutoUpdate)
        self.btn_stopAutoUpdate.grid(row=0, column=7, sticky=tkr.EW, padx=3, pady=3)
        self.btn_stopAutoUpdate.config(state='disabled')


        y +=1
        
        btn_frame = tkr.Frame(self)
        btn_frame.grid(row=y, column=0, sticky=(tkr.E, tkr.W, tkr.N, tkr.S))
        self.restoreDefaults = tkr.Button(btn_frame, text=_('Restore Defaults'), command=self.restoreDefaults_func)
        self.restoreDefaults.grid(row=0, column=0, sticky=tkr.EW, padx=1)
        self.update_btn = tkr.Button(btn_frame, text=_('Update DB'), command=self.updateDB_btn_func)
        self.update_btn.grid(row=0, column=1, sticky=tkr.EW, padx=1)
        
        makeWeight(btn_frame)
        makeWeight(self)
        makeWeight(firstGroupFrame)
        makeWeight(secondGroupFrame)
        makeWeight(durationFrame)
        makeWeight(thirdGroupFrame)

    def _stopAutoUpdate(self):
        return self.rssDBFrame.stopAutoUpdate()

    def restoreDefaults_func(self):
        self.vals = dict()
        self.fromDateVar.set('')
        self.toDateVar.set('')
        self.maxWorkersVar.set(os.cpu_count()/2)
        self.timeOutVar.set(3)
        self.retriesVar.set(3)
        self.getXML_cb.valueVar.set(0)
        self.getLatest_cb.valueVar.set(1)
        self.reloadCache_cb.valueVar.set(0)
        self.updateTickerMapping_cb.valueVar.set(1)
        self.getFilersInfo_cb.valueVar.set(1)
        self.updateFilersInfo_cb.valueVar.set(1)
        self.refreshFilersInfo_cb.valueVar.set(0)

        self.setAutoUpdate_cb.valueVar.set(0)
        self.waitForVar.set(10)
        self.daysVar.set(0)
        self.hoursVar.set(1)
        self.minutesVar.set(0)
        return


    def updateDB_btn_func(self):
        if not self.rssDBFrame.dbConnection or not self.rssDBFrame.dbConnection.checkConnection():
            tkr.messagebox.showerror(_("Dialog validation error(s)"), 'Database connection is not available', parent=self)
            return
        try:
            self.dateFrom = self.fromDateVar.get() if self.fromDateVar.get() else None
            self.dateTo = self.toDateVar.get() if self.toDateVar.get()  else None
            self.maxWorkers = self.maxWorkersVar.get() if self.maxWorkersVar.get() else os.cpu_count()/2 
            self.timeOut = self.timeOutVar.get() if self.timeOutVar.get() else 3
            self.retries = self.retriesVar.get() if self.retriesVar.get() else 3
            self.getXML = self.getXML_cb.value
            self.includeLatest = self.getLatest_cb.value
            self.reloadCache = self.reloadCache_cb.value
            self.updateTickers = self.updateTickerMapping_cb.value
            self.getFilers = self.getFilersInfo_cb.value
            self.updateExisting = self.updateFilersInfo_cb.value
            self.refreshAll = self.refreshFilersInfo_cb.value
            self.setAutoUpdate = self.setAutoUpdate_cb.value
            self.waitFor = timedelta(minutes=self.waitForVar.get())
            self.duration = timedelta(days=self.daysVar.get(), hours=self.hoursVar.get(), minutes=self.minutesVar.get()) 

        except Exception as e:
            tkr.messagebox.showerror(_("Dialog validation error(s)"), str(e), parent=self)
            return

        # validate dates
        for k,v in {'From': self.dateFrom, 'To': self.dateTo}.items():
            if v:
                try:
                    datetime.datetime.strptime(v, '%Y-%m-%d')
                except:
                    tkr.messagebox.showerror(_("Dialog validation error(s)"), _('{} Date is not in the correct fromat, date should be in the format yyyy-mm-dd').format(k), parent=self)
                    return
        if (self.dateFrom and self.dateTo) and (datetime.datetime.strptime(self.dateTo, '%Y-%m-%d') <= datetime.datetime.strptime(self.dateFrom, '%Y-%m-%d')):
            tkr.messagebox.showerror(_("Dialog validation error(s)"), 'To Date must be later than From date', parent=self)
            return

        if self.maxWorkers > os.cpu_count():
            tkr.messagebox.showerror(_("Dialog validation error(s)"), 'Max number of workers for this machine is {}'.format(str(os.cpu_count())), parent=self)
            return
        
        # if self.waitForVar.get() < 10:
        #     tkr.messagebox.showerror(_("Dialog validation error(s)"), 'Check duration should be a least 10 minutes {} entered'.format(self.waitFor), parent=self)

        self.vals = {x:getattr(self, x) for x in self.params}
        self.accepted = True
        # print(self.vals)

        try:
            self.rssDBFrame.backGroundUpdateDB(self.vals)
        except Exception as e:
            tkr.messagebox.showerror(title='RSS DB Error - updateDB', message=str(e) + '\n' + traceback.format_exc())
        return

    # def cancel_btn_func(self):
    #     self.master.destroy()

class rssDBSearchDBPanel(tkr.LabelFrame):
    def __init__(self, master:rssDBFrame, **kw):
        self.accepted = False
        self.vals = dict()
        self.params = ['companyName', 'tickerSymbol', 'cikNumber', 'formType', 'assignedSic', 'dateFrom', 'dateTo', 'inlineXBRL', 'limit']
        self.companyNameVar = tkr.StringVar()
        self.companyNameVar.set('')
        self.tickerSymbolVar = tkr.StringVar()
        self.tickerSymbolVar.set('')
        self.cikNumberVar = tkr.StringVar()
        self.cikNumberVar.set('')
        self.formTypeVar = tkr.StringVar()
        self.formTypeVar.set('')
        self.assignedSicVar = tkr.StringVar()
        self.assignedSicVar.set('')
        self.dateFromVar = tkr.StringVar()
        self.dateFromVar.set('')
        self.dateToVar = tkr.StringVar()
        self.dateToVar.set('')
        self.isInlineXbrlVar = tkr.StringVar()
        self.isInlineXbrlVar.set('all')
        self.limitVar = tkr.IntVar()
        self.limitVar.set(100)

        super().__init__(master=master, **kw)
        y = 0
        firstGroupFrame = tkr.LabelFrame(self, text = _('Filing Date Range and Query Limit (Result limiter)') , padx=5, pady=5)
        firstGroupFrame.grid(column=0, row=y, padx=3, pady=3, sticky=tkr.EW)
        y_1 = 0
        dateRangeLabel = tkr.Label(firstGroupFrame, text=_('Filing Date -->'))
        dateRangeLabel.grid(row=y_1, column=0, sticky=tkr.W, pady=3, padx=3)
        fromDateLabel = tkr.Label(firstGroupFrame, text=_('From:'))
        fromDateLabel.grid(row=y_1, column=1, sticky=tkr.E, pady=3, padx=3)
        self.fromDateEntry = tkr.Entry(firstGroupFrame, textvariable=self.dateFromVar, name='fromDateEntry')
        self.fromDateEntry.grid(row=y_1, column=2, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.fromDateEntry, _('Filing date in the format: yyyy-mm-dd, if left empty, gets from the earliest filing date, filings are sorted descending by filing date.'))
        toDateLabel = tkr.Label(firstGroupFrame, text=_('To:'))
        toDateLabel.grid(row=y_1, column=3, sticky=tkr.E, pady=3, padx=3)
        self.toDateEntry = tkr.Entry(firstGroupFrame, textvariable=self.dateToVar, name='toDateEntry')
        self.toDateEntry.grid(row=y_1, column=4, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.toDateEntry, _('Filing date in the format: yyyy-mm-dd, if left empty, gets up to the latest filing date, filings are sorted descending by filing date.'))
        
        y_1 +=1
        self.formTypeLabel = tkr.Label(firstGroupFrame, text=_('form Type:'))
        self.formTypeLabel.grid(row=y_1, column=0, sticky=tkr.W, pady=3, padx=3)
        self.formTypeEntry = tkr.Entry(firstGroupFrame, textvariable=self.formTypeVar, name='formTypeEntry')
        self.formTypeEntry.grid(row=y_1, column=1, columnspan=4, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.formTypeEntry, text=_("Comma separated SEC form type, example: 10-K, 10-Q,..."), wraplength=360) 

        y_1 +=1
        assignedSicLabel = tkr.Label(firstGroupFrame, text=_('Select Standard Industry Classification(s) (SIC):'))
        assignedSicLabel.grid(row=y_1, column=0, columnspan=4, sticky=tkr.W, pady=3, padx=3)
        self.btn_industrySelect = tkr.Button(firstGroupFrame,text=_("Select Industry"), command=self.openIndustrySelector)
        self.btn_industrySelect.grid(row=y_1, column=4, columnspan=2,sticky=tkr.EW, pady=1, padx=1)
        ToolTip(self.btn_industrySelect, text=_("Select From SEC Standard Industry classifications in addition to the other fields"), wraplength=360) 
        y_1+=1
        self.assignedSicEntry = tkr.Entry(firstGroupFrame, textvariable=self.assignedSicVar, name='assignedSicEntry', disabledbackground='white')
        self.assignedSicEntry.grid(row=y_1, column=0, columnspan=6, sticky=tkr.EW, pady=3, padx=3)

        y_1 += 1
        isInlineXbrl = tkr.Label(firstGroupFrame, text=_("Inline XBRL:"), underline=0, name='isInlineXbrl')
        isInlineXbrl.grid(row=y_1, column=0, columnspan=2, sticky=tkr.E, pady=3, padx=3)
        cbIsInlineXbrl = ttk.Combobox(firstGroupFrame, textvar=self.isInlineXbrlVar, values = ('yes', 'no', 'all'), state='readonly', name='cbIsInlineXbrl')
        cbIsInlineXbrl.set('all')
        cbIsInlineXbrl.grid(row=y_1, column=2, sticky=tkr.W, pady=3, padx=3)

        limitLabel = tkr.Label(firstGroupFrame, text=_('Result Limit:'))
        limitLabel.grid(row=y_1, column=3, sticky=tkr.E, pady=3, padx=3)
        self.limitEntry = tkr.Entry(firstGroupFrame, textvariable=self.limitVar, name='limitEntry')
        self.limitEntry.grid(row=y_1, column=4, sticky=tkr.W, pady=3, padx=3)
        ToolTip(self.limitEntry, text=_("Limits the number of rows returned by query"), wraplength=360)

        y +=1
        y_2 = 0
        secondGroup = tkr.LabelFrame(self, text = _('Entity Selection (All matches for each field are returned)'), padx=5, pady=5)
        secondGroup.grid(column=0, row=y, padx=3, pady=3, sticky=tkr.EW)

        companyNameLabel = tkr.Label(secondGroup, text=_('Filer(s) Name(s):'))
        companyNameLabel.grid(row=y_2, column=0, sticky=tkr.W, pady=3, padx=3)
        self.companyNameEntry = tkr.Entry(secondGroup, textvariable=self.companyNameVar, name='companyNameEntry')
        self.companyNameEntry.grid(row=y_2, column=1, columnspan=5, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.companyNameEntry, text=_("Comma separated companies names or part there of, example: microsoft, General Electric,... \n Looks for the exact name or a similar name."), wraplength=360) 

        y_2+=1
        tickerSymbolLabel = tkr.Label(secondGroup,  text=_('ticker(s) Symbol(s):'))
        tickerSymbolLabel.grid(row=y_2, column=0, sticky=tkr.W, pady=3, padx=3)
        self.tickerSymbolEntry = tkr.Entry(secondGroup, textvariable=self.tickerSymbolVar, name='tickerSymbolEntry')
        self.tickerSymbolEntry.grid(row=y_2, column=1, columnspan=5, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.tickerSymbolEntry, text=_("Comma separated tickers, example: msft, gm,... \n Looks for the exact ticker symbol(s) in addition to company names in the company names field."), wraplength=360) 

        y_2+=1
        cikNumberLabel = tkr.Label(secondGroup, text=_('CIK Number(s):'))
        cikNumberLabel.grid(row=y_2, column=0, sticky=tkr.W, pady=3, padx=3)
        self.cikNumberEntry = tkr.Entry(secondGroup, textvariable=self.cikNumberVar, name='cikNumberEntry')
        self.cikNumberEntry.grid(row=y_2, column=1, columnspan=5, sticky=tkr.EW, pady=3, padx=3)
        ToolTip(self.cikNumberEntry, text=_("Comma separated cik Numbers, example: 0001234567, 0007654321,... \n Looks for the exact CIK Number(s) in addition to ticker symbol(s) and companies names."), wraplength=360) 

        y += 1
        btn_frame = tkr.Frame(self)
        btn_frame.grid(row=y, column=0, sticky=(tkr.E, tkr.W, tkr.N, tkr.S))
        self.clearForm_btn = tkr.Button(btn_frame, text=_('Clear'), command=self.clearForm)
        self.clearForm_btn.grid(row=0, column=0, columnspan=3, sticky=tkr.EW, padx=1)
        self.searchDB_btn = tkr.Button(btn_frame, text=_('Search DB'), command=self.searchDB)
        self.searchDB_btn.grid(row=0, column=3, columnspan=3,sticky=tkr.EW, padx=1)


        makeWeight(btn_frame)
        makeWeight(self)
        makeWeight(firstGroupFrame)
        makeWeight(secondGroup)
    
    def clearForm(self):
        self.companyNameVar.set('')
        self.tickerSymbolVar.set('')
        self.cikNumberVar.set('')
        self.formTypeVar.set('')
        self.assignedSicVar.set('')
        self.dateFromVar.set('')
        self.dateToVar.set('')
        self.limitVar.set(100)
        return

    def searchDB(self):
        if not self.rssDBFrame.dbConnection or not self.rssDBFrame.dbConnection.checkConnection():
            tkr.messagebox.showerror(_("Dialog validation error(s)"), 'Database connection is not available', parent=self)
            return
        try:
            self.companyName = self.companyNameVar.get() if self.companyNameVar.get() else None 
            self.tickerSymbol = self.tickerSymbolVar.get() if self.tickerSymbolVar.get() else None
            self.cikNumber = self.cikNumberVar.get() if self.cikNumberVar.get() else None
            self.formType = self.formTypeVar.get() if self.formTypeVar.get() else None
            self.assignedSic = self.assignedSicVar.get() if self.assignedSicVar.get() else None
            self.dateFrom = self.dateFromVar.get() if self.dateFromVar.get() else None
            self.dateTo = self.dateToVar.get() if self.dateToVar.get() else None
            self.inlineXBRL = None
            if self.isInlineXbrlVar.get().lower() in ('yes', 'no'):
                self.inlineXBRL = self.isInlineXbrlVar.get()
            self.limit= self.limitVar.get() if self.limitVar.get() else 100
        except Exception as e:
            tkr.messagebox.showerror(_("Dialog validation error(s)"), str(e), parent=self)
            return

        for k,v in {'From': self.dateFrom, 'To': self.dateTo}.items():
            if v:
                try:
                    datetime.datetime.strptime(v, '%Y-%m-%d')
                except:
                    tkr.messagebox.showerror(_("Dialog validation error(s)"), '{} Date is not in the correct fromat, date should be in the format yyyy-mm-dd'.format(k), parent=self)
                    return
        if (self.dateFrom and self.dateTo) and (datetime.datetime.strptime(self.dateTo, '%Y-%m-%d') <= datetime.datetime.strptime(self.dateFrom, '%Y-%m-%d')):
            tkr.messagebox.showerror(_("Dialog validation error(s)"), 'To Date must be later than From date', parent=self)
            return

        self.vals = {x:getattr(self, x) for x in self.params}
        self.accepted = True
        self.searchDB_btn.config(state='disabled')
        t = threading.Thread(target=self.rssDBFrame.backgroundSearchDB, args=(self.vals,), daemon=True)
        t.start()
        return
        
    def openIndustrySelector(self):
        global industryCodesSelection
        selector = industrySelector(self, self.btn_industrySelect)
        if industryCodesSelection:
            self.assignedSicVar.set('')
            self.assignedSicVar.set(', '.join(industryCodesSelection))
        return

