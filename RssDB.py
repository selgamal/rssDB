"""Database connection classes for rssDB
implemented database products:
    - Sqlite
    - Postgresql
    - MongoDB
"""
import sys, os, re, time, glob, io, json, gettext, gc, tempfile, logging, calendar, concurrent.futures, threading, traceback, pickle
from tkinter.filedialog import SaveAs
from concurrent.futures import as_completed
from collections import OrderedDict
from datetime import datetime, date, timedelta
from dateutil import parser, tz
from calendar import monthrange
from lxml import html, etree
from urllib import request
from arelle import ModelXbrl, XmlUtil
from arelle.PythonUtil import flattenSequence
from arelle.CntlrCmdLine import CntlrCmdLine
from .Constants import pathToSQL, wait_duration, DBTypes, rssTables, rssCols, RSSFEEDS
from .CommonFunctions import updateCikTickerMapping, _populateFilersInfo, _doAll,\
    getFilerInformation, _getMonthlyFeedsLinks, _getFeedInfo, getRssItemInfo, _startDBReport

try:
    from xbrlDB.SqlDb import SqlDbConnection, XPDBException, pg8000
except:
    try:
        from arelle.plugin.xbrlDB.SqlDb import SqlDbConnection, XPDBException, pg8000
    except:
        from plugin.xbrlDB.SqlDb import SqlDbConnection, XPDBException, pg8000

try:
    from arellepy.HelperFuncs import chkToList, convert_size, xmlFileFromString
    from arellepy.CntlrPy import CntlrPy, subProcessCntlrPy, renderEdgarReportsFromRssItems
    from arellepy.LocalViewerStandalone import initViewer
except:
    from .arellepy.HelperFuncs import chkToList, convert_size, xmlFileFromString
    from .arellepy.CntlrPy import CntlrPy, subProcessCntlrPy, renderEdgarReportsFromRssItems
    from .arellepy.LocalViewerStandalone import initViewer

hasMongoDB = True

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
except Exception as e:
    hasMongoDB = False

TRACESQLFILE = None

sqlScriptsFiles = {
    'sqlite': ['sqliteCreateRssDB.sql', 'industryClassificationDataInsert.sql'],
    'postgres': ['postgreCreateRssDB.sql', 'industryClassificationDataInsert.sql']
}

mongodbSchemaFile = os.path.join(pathToSQL, 'mongodbSchema.json')
mongodbIndustryClassificationFile = os.path.join(pathToSQL, 'mongodbIndustryClassification.json')

MAKEDOTS_RSSDB = False 

def dotted(cntlr, xtext='Processing'):
    '''Just let me know you are alive!'''
    global MAKEDOTS_RSSDB
    n = 1
    while MAKEDOTS_RSSDB:
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

def rssDBConnection(cntlr, **kwargs):
    '''Shortcut to connect to db, returns connection object of the appropriate type
    
    kwargs:
        user: database user name, not relevant to sqlite 
        password: database password, not relevant to sqlite 
        host: localhost or computer hosing the database, not relevant to sqlite 
        port: int for port number on host 
        database: database name 
        timeout: time out for trying to connect 
        product: one of 'postgres', 'sqlite', 'mongodb'
        schema: in case of postgres a schema must to be provided
        createSchema: in case of prostgres, whether to create a schema and tables (initialize tables)
        createDB: whether to create database and tables, not relevant to postgres    
    '''
    gettext.install('arelle')
    # cntlr.addToLog('Platform :{} Platform: {} GUI: {}'.format(sys.platform.lower().startswith('win'), sys.platform.lower(), cntlr.hasGui) )
    global hasMongoDB, MongoClient, ASCENDING, DESCENDING
    if not kwargs.get('product') in DBTypes:
        raise Exception('product must be on of {} {} was entered'.format(', '.join(DBTypes), kwargs.get('product')))
    
    conParams = {
        'user': kwargs.get('user', None), 
        'password': kwargs.get('password', None), 'host': kwargs.get('host', None), 
        'port': kwargs.get('port', None), 'database': kwargs.get('database', None), 
        'timeout': kwargs.get('timeout', None), 'product': kwargs.get('product', None), 'schema': kwargs.get('schema', None),
        'createSchema': kwargs.get('createSchema', None), 'createDB': kwargs.get('createDB', None),
        }

    dbConn = None
    if kwargs.get('product') in ('postgres', 'sqlite'):
        dbConn = rssSqlDbConnection(cntlr, **conParams)
    elif kwargs.get('product') == 'mongodb':
        # check if required packages exists
        # first make sure that additional path entries in config file are added
        rssDBaddToSysPath = cntlr.config.setdefault('rssDBaddToSysPath', [])
        for p in rssDBaddToSysPath:
            if not p in sys.path:
                sys.path.append(p)
        try:
            from pymongo import MongoClient, ASCENDING, DESCENDING
            hasMongoDB = True
        except Exception as e:
            hasMongoDB = False        
        pyMongoPath = []
        fail_msg = _("No path was provided")
        fail_msg_2 = _('pymongo package required to use this feature is not available')
        if not hasMongoDB:
            if cntlr.hasGui:
                from tkinter import filedialog
                from tkinter import messagebox
                getpath = messagebox.askyesno(
                    title=(_('RSS DB Connection Error')),
                    message = ('pymongo package needed for mongodb connection is not avaliable '
                            'in the current installation of arelle.\n\n'
                            'Do you want to add an installed pymongo to path? Must be for python {}.{}').format(sys.version_info.major, sys.version_info.minor),
                            icon='warning', parent=cntlr.parent)
                if getpath:
                    pyMongoPath = filedialog.askdirectory(title=_('Select pymongo installation dir'), parent=cntlr.parent)
                    if pyMongoPath and os.path.isdir(pyMongoPath):
                        if not pyMongoPath in sys.path:
                            sys.path.append(pyMongoPath)
                    else:
                        cntlr.addToLog(fail_msg, messageCode="RssDB.Info", file="",  level=logging.INFO)
                        messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg, parent=cntlr.parent)
                        return

                    try:
                        from pymongo import MongoClient, ASCENDING, DESCENDING
                        hasMongoDB = True
                        _pyMongoPath = chkToList(pyMongoPath, str)
                        for p in _pyMongoPath:
                            if p not in rssDBaddToSysPath:
                                rssDBaddToSysPath.append(p)
                        cntlr.saveConfig()
                    except Exception as e:
                        hasMongoDB = False
                        cntlr.addToLog(fail_msg_2, messageCode="RssDB.Error", file="",  level=logging.ERROR)
                        messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg_2 +'\n'+str(e), parent=cntlr.parent)      
                else:
                    cntlr.addToLog(fail_msg, messageCode="RssDB.Info", file="",  level=logging.INFO)
                    messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg, parent=cntlr.parent)
                    return
            else:
                cntlr.addToLog(fail_msg_2, messageCode="RssDB.Error", file="",  level=logging.ERROR)
                cntlr.showStatus(_('Install required packages or add installation path to "sys.path"'))
                return

        dbConn = rssMongoDbConnection(cntlr, **conParams)

    return dbConn

def _getFeedInfoHelper(setConfigDir, targetResDir, conParams, feedLink, lastModifiedDate, isNew, product, 
                        insertIntoDB=False, reloadCache=False, getFiles=True, getXML=False, returnInfo=False, isLatest=False, q=None):
    """helper function for concurrent executor gets feed info ready to insert in db"""
    import gettext, datetime, time
    conn = None
    cntlr = None
    startAllTime = time.perf_counter()

    cntlr = subProcessCntlrPy(
        instConfigDir=setConfigDir,
        useResDir=targetResDir,
        logFileName="logToBuffer",
        loadPlugins=False,        
        q=q
    )
    conParams['cntlr'] = cntlr
    conn = None
    if product in ('sqlite', 'postgres'):
        conn = rssSqlDbConnection(**conParams)
    elif product == 'mongodb':
        conn = rssMongoDbConnection(**conParams) 

    if isLatest:
        conn.showStatus(_('Getting Latest Filings'))

    info = conn.getFeedInfo(feedLink, lastModifiedDate, isNew, reloadCache, getFiles, getXML)
    
    flatenFiles = []
    if len(info[rssTables[2]])>0:
        flatenFiles = [y for x in info[rssTables[2]] for y in x]
        info[rssTables[2]] = flatenFiles

    insertUpdateStats = {x:{'insert':0, 'update':0} for x in rssTables} 
    _feedInfo = dict()
    if isLatest: # do not re-insert feedsInfo entry if it exists
        feedsIds = conn.getExistingFeeds()
        if info[rssTables[0]][rssCols[rssTables[0]][0]] in feedsIds:
            _feedInfo =  info.pop(rssTables[0])
            _feedInfo['Status'] = 'Month feed already in DB'
            # but ensure stats reflects this feed was updated
            insertUpdateStats[rssTables[0]]['update'] = 1 if any([len(v) for k, v in info.items() if k in rssTables]) else 0
        else:
            # in case of first day of the month before the monthly page is updated
            info['isNew'] = True
    
     
    if insertIntoDB:
        _action='update' if not info['isNew'] else 'insert'
        try:
            if isLatest:
                conn.showStatus(_('Inserting Latest Filings'))
            for _tbl, data in info.items():
                if _tbl in rssTables and len(data)>0:
                    insertUpdateStats[_tbl] = conn.insertUpdateRssDB(data, _tbl, _action if _tbl == rssTables[0] else 'insert' , returnStat=True)
            if conn.product in ['sqlite', 'postgres']:
                conn.commit()
        except Exception as e:
            if conn.product in ['sqlite', 'postgres']:
                conn.rollback()
            raise e
    
    if isLatest and _feedInfo:
        info[rssTables[0]] = _feedInfo

    _feed = {rssTables[0]: info[rssTables[0]]} if isLatest else None
    results = {'link': feedLink,'stat': insertUpdateStats, 'feed': _feed}
    
    results['logMsg'] = _("Finished extracting data and inserting into db {} secs").format(round(time.perf_counter() - startAllTime, 3))
    conn.addToLog(results['logMsg'], messageCode="RssDB.Info", file=feedLink,  level=logging.INFO)
    
    if returnInfo:
        results['feed'] = info
    else:
        del flatenFiles
        del info
    try:
        logs = cntlr.logHandler.getLines()
    except:
        pass
    cntlr.modelManager.close()

    del cntlr, conParams, conn
    gc.collect()
    return results

def _updateRssFeeds(conn, loc=None, getRssItems=False, updateDB=False, maxWorkers=None, returnInfo=False,
                    dateFrom=None, dateTo=None, last=None, reloadCache=False, includeLatest=False, getFiles=True, getXML=False, q=None):
    """Checks for new feeds and rss items, initializes and/or updates rss DB tables if specified"""
    global MAKEDOTS_RSSDB
    if not maxWorkers:
        # use half of available cpus
        maxWorkers = os.cpu_count()/2
    conParams = conn.conParams
    startTime = time.perf_counter()
    links = conn.getMonthlyFeedsLinks(loc=loc, maxWorkers=maxWorkers, last=last, dateFrom=dateFrom, dateTo=dateTo)
    feeds = []
    if getRssItems:
        setConfigDir = os.path.dirname(conn.cntlr.userAppDir)
        targetResDir = os.path.dirname(conn.cntlr.imagesDir)
        if updateDB:
            # make sure tables/schema are created
            if conn.product == 'mongodb':
                conn.verifyCollections(createCollections=True)
            else:
                conn.verifyTables(createTables=True)
        
        if sys.platform.lower().startswith('win'):
            # then we are in windows world
            conn.addToLog(_('Not using multiprocessing for extracting feed data'), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
            if conn.cntlr.hasGui:
                MAKEDOTS_RSSDB = True
                t = threading.Thread(target=dotted, args=(conn.cntlr,), daemon=True)
                t.start()
            _links = [(x['link'], x.get('lastModifiedDate', None), x.get('isNew', None)) for x in links]
            for l, d, n in _links:
                res = _getFeedInfoHelper(setConfigDir, targetResDir, conParams, l, d, n, conn.product, updateDB, reloadCache, getFiles, getXML, returnInfo, False, q)
                feeds.append(res)
                conn.addToLog(res['logMsg'], messageCode="RssDB.Info", file=l,  level=logging.INFO)
            
            latest = None
            if includeLatest:
                latest = _getFeedInfoHelper(setConfigDir, targetResDir, conParams, RSSFEEDS['US SEC All Filings'], datetime.min, False, conn.product, updateDB,
                                                    reloadCache, getFiles, getXML, returnInfo, True, q)
                conn.addToLog('Latest: ' + latest['logMsg'], messageCode="RssDB.Info", file=RSSFEEDS['US SEC All Filings'],  level=logging.INFO)
                feeds.append(latest)
            if latest:
                latestLink = {'feedId': latest['feed']['feedsInfo'].get('feedId'),
                                'feedDate': latest['feed']['feedsInfo'].get('feedMonth'),
                                'link': latest['feed']['feedsInfo'].get('link'),
                                'lastModifiedDate': None,
                                'lastBuildDate': latest['feed']['feedsInfo']['lastBuildDate'],
                                'isNew': False
                                }
                links.append(latestLink)

        else:
            conn.addToLog(_('Using multiprocessing for extracting feed data with maxWorkers: {}').format(maxWorkers), messageCode="RssDB.Info", 
                                file=conn.conParams.get('database',''),  level=logging.INFO)
            with concurrent.futures.ProcessPoolExecutor(max_workers=int(maxWorkers)) as executor:
                argLen = len(links)
                a1 = [setConfigDir] * argLen
                a2 = [targetResDir] * argLen
                a3 = [conParams] * argLen
                a4 = [x['link'] for x in links]
                a5 = [x['lastModifiedDate'] if 'lastModifiedDate' in x.keys() else None for x in links]
                a6 = [x['isNew'] for x in links]
                a7 = [conn.product] * argLen
                a8 = [updateDB] * argLen
                a9 = [reloadCache] * argLen
                a10 = [getFiles] * argLen
                a11 = [getXML] * argLen
                a12 = [returnInfo] * argLen
                a13 = [False] * argLen
                # _feeds = executor.map(_getFeedInfoHelper, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, [q] * argLen)
                argZ = zip(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, [q] * argLen)
                __feeds = [executor.submit(_getFeedInfoHelper, *x) for x in zip(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, [None] * argLen) ]
                _feeds = []
                
                if conn.cntlr.hasGui:
                    MAKEDOTS_RSSDB = True
                    t = threading.Thread(target=dotted, args=(conn.cntlr,), daemon=True)
                    t.start()

                for x in as_completed(__feeds):
                    conn.addToLog(x.result()['logMsg'], messageCode="RssDB.Info", file=x.result()['link'],  level=logging.INFO)
                    _feeds.append(x.result())
                feeds = list(_feeds)
                latest = None
                if includeLatest:
                    _latest = executor.submit(_getFeedInfoHelper, setConfigDir, targetResDir, conParams, RSSFEEDS['US SEC All Filings'],
                                                datetime.min, False, conn.product, updateDB,
                                                reloadCache, getFiles, getXML, returnInfo, True, None)
                    latest = _latest.result()
                    conn.addToLog('Latest: ' + latest['logMsg'], messageCode="RssDB.Info", file=RSSFEEDS['US SEC All Filings'],  level=logging.INFO)
                    feeds.append(latest)
                if latest:
                    latestLink = {'feedId': latest['feed']['feedsInfo'].get('feedId'),
                                    'feedDate': latest['feed']['feedsInfo'].get('feedMonth'),
                                    'link': latest['feed']['feedsInfo'].get('link'),
                                    'lastModifiedDate': None,
                                    'lastBuildDate': latest['feed']['feedsInfo']['lastBuildDate'],
                                    'isNew': False
                                    }
                    links.append(latestLink)
    if MAKEDOTS_RSSDB:
        MAKEDOTS_RSSDB = False
    summaryList = [x['stat'] for x in feeds]
    summaryTotals = dict()
    for t in rssTables:
        updates = []
        inserts = []
        for s in summaryList:
            if s[t].get('update'):
                updates.append(s[t].get('update'))
            if s[t].get('insert'):
                inserts.append(s[t].get('insert'))
        summaryTotals[t] = {'insert': sum(inserts), 'update': sum(updates)}
        
    _msg =  _('Finished updating RSS Feeds in {} secs').format(round(time.perf_counter()-startTime, 3), 
                messageCode="RssDB.Info", file="",  level=logging.INFO)
    result = {'summary': summaryTotals, 'stats': _msg}
    if returnInfo:
        result['links'] = links
        result['feeds'] = [x['feed'] for x in feeds if x['feed']]
    else:
        del feeds, links
    conn.addToLog(_msg, messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    return result

class rssSqlDbConnection(SqlDbConnection):
    """Few modifications to sqlDBConnection class"""
    def __init__(self, cntlr, user, password, host, port, database, timeout, product, schema, createSchema=False, createDB=False):
        self.cntlr = cntlr
        self.autoUpdateSet = False
        self.updateStarted = False
        self.updateStopped = False
        _modelXbrl = cntlr.modelManager.modelXbrl
        if not _modelXbrl:
            _modelXbrl = ModelXbrl.ModelXbrl(cntlr.modelManager)
        if createDB:
            if product == 'postgres':
                self.addToLog(_('Cannot create database on postgres can only create schema, database needs to be created on the server'), messageCode="RssDB.Error", file=database,  level=logging.ERROR)
                raise Exception('Cannot create database on postgres, can only create schema, database needs to be created on the server')
        elif not createDB and product == 'sqlite' and not database == ':memory:':
            if not os.path.exists(database):
                self.addToLog(_('Database "{}" does not Exist').format(database), messageCode="RssDB.Error",  file=database,  level=logging.ERROR)
                raise Exception('Database "{}" does not Exist'.format(database))

        self.conParams = {'cntlr': None, 'user': user, 
                            'password': password, 'host': host, 
                            'port': port, 'database': database, 
                            'timeout': timeout, 'product': product, 'schema': schema}
        super().__init__(_modelXbrl, user, password, host, port, database, timeout, product)
        if self.product in ['postgres']:
            if not schema:
                schema = 'rssFeeds'
            try:
                _schemas = self.execute('select schema_name from information_schema.schemata;', fetch=True)
                schemas = [s[0] for s in _schemas]                
                if not schema in schemas:
                    if createSchema:
                        stat = _('Creating Schema {}').format(schema)
                        self.showStatus(stat, 2000)
                        self.execute('CREATE SCHEMA IF NOT EXISTS "{}";'.format(
                            schema), action=stat, fetch=False, commit=True)
                    else:
                        raise Exception('Schama {} does not exist in {} database'.format(schema, database))
                self.execute('SET search_path = "{}";'.format(schema), fetch=False, commit=True)
                self.showStatus(_('Path set to {}').format(schema), 2000)
            except Exception as e:
                self.rollback()
                raise e
        self.schema = schema
        self.conParams['schema'] = schema

        if product=='postgres' and createSchema:
            chkTables = self.verifyTables(createTables=False, dropPriorTables=False)
            if not chkTables:
                self.create([os.path.join(pathToSQL, f) for f in sqlScriptsFiles[self.product]], dropPriorTables=False)

        if product == 'sqlite' and createDB:
            chkTables = self.verifyTables(createTables=False, dropPriorTables=False)
            if not chkTables:
                self.create([os.path.join(pathToSQL, f) for f in sqlScriptsFiles[self.product]], dropPriorTables=False)

        chk = self.checkConnection()
        if not chk:
            self.close()
            raise Exception('Could not connet to database {}'.format(database))
            return


    def getFormulae(self):
        qry = self.execute('select "formulaId", "description", "fileName", "dateTimeAdded" from formulae', fetch=True, close=False)
        cols = [x[0].decode() if type(x[0]) is bytes else x[0] for x in self.cursor.description]
        res = [dict(zip(cols, x) )for x in qry]
        return res


    def addFormulaToDb(self, fileName=None, formulaId=None, description=None, formulaLinkBaseString=None, replaceExistingFormula=False, returnData=True):
        '''Inserts a new or updates existing formula in the database

        Cannot have duplicate formulaId OR fileName in db, checks if filename already exists in database, if exists and `replaceExistingFormula` set to False,
        file is not added, if `replaceExistingFormula` set to True, the database entry is updated with the current data. 

        Formula can be read as string via `formulaLinkBaseString` parameter directly, if only filename is provided the file is read from drive.

        At least one of fileName or formulaLinkbaseString must be entered.

        args:
            fileName: path to formula linkbase file on local drive or just a unique name if `formulaLinkBaseString` is used
            description: a brief description of what the function does, defaults to file basename
            formulaLinkBaseString: Sting representing a valid formula linkbase
            replaceExisting: if there is an entry with the same file name in the db, and this parameter is set to true, the existing entry is replace with current data    
        '''
        pgParamStyle = None
        if self.product == 'postgres':
            pgParamStyle = pg8000.paramstyle
            pg8000.paramstyle = 'qmark'
        noFile = 'NO_FILE'
        if not any([fileName, formulaLinkBaseString]):
            self.cntlr.addToLog(_('At least one of fileName or formulaLinkbaseString must be entered.'), messageCode="RssDB.Error", file=self.conParams.get('database',''), level=logging.ERROR)
            return
        action = 'insert'
        formulaData = dict()
        if fileName is None:
            fileName = noFile

        _existingFiles = []
        existingFormulaId = []
        existingFileNames = []

        if any([fileName, formulaId]):
            f_qry = 'SELECT "formulaId", "fileName" from "formulae" WHERE '
            params = tuple()
            if formulaId is None and fileName:
                f_qry += '"fileName"=?'
                params = (fileName,)
            elif fileName is None and formulaId:
                f_qry += '"formulaId"=?'
                params = (formulaId,)
            elif all([fileName, formulaId]):
                f_qry += '"formulaId"=? OR "fileName"=?'
                params = (formulaId, fileName)

            _existingFiles = self.execute(f_qry, params=params, fetch=True)
        if _existingFiles:
            existingFormulaId = [x for x in _existingFiles if x[0]==formulaId]
            existingFileNames = [x for x in _existingFiles if x[1]==fileName]
        if existingFormulaId or existingFileNames:
            existingIds = [x[0] for x in existingFileNames]
            if replaceExistingFormula:
                action = 'update'
                if not formulaId or formulaId not in existingIds:
                    self.cntlr.addToLog(_('Enter an existing formulaId to update, existing ids: {}').format(str(existingIds)), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
                    return
            else:
                idMsg = 'formula with formulaId {}'.format(str(formulaId)) if len(existingFormulaId) else ''
                fNameMsg = '{} formula(e) with fileName {} with formulaId(s) {}'.format(str(len(existingFileNames)), fileName, str(existingIds)) if len(existingFileNames) else ''
                self.cntlr.addToLog(_('DB has already {}{}{}, set "replaceExistingFormula" to True and enter a formulaId to update an existing formula by Id').format(
                    idMsg, ' and ' if idMsg and fNameMsg else '', fNameMsg), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
                return
        if action == 'insert':
            # get new id if id is not chosen
            if not formulaId:
                _ids = [x[0] for x in self.execute('SELECT max("formulaId") from "formulae"', fetch=True) if x[0]]
                formulaId = max(_ids) + 1 if _ids else 1000

        lb = ''
        if formulaLinkBaseString:
            lb = formulaLinkBaseString
        elif os.path.isfile(fileName):
            with open(fileName, 'r') as fp:
                lb = fp.read().replace('\n', '')

        if lb:
            lb_xml = etree.fromstring(lb).getroottree()
            lb_string = etree.tostring(lb_xml) # , pretty_print=True, encoding=lb_xml.docinfo.encoding if lb_xml.docinfo.encoding else None
            formulaLinkBaseString = lb_string.decode(lb_xml.docinfo.encoding)
        else:
            raise Exception('No valid file path or formula linkbase string was provided')
                
        formulaData = {
            'formulaId': formulaId,
            'fileName': fileName,
            'description': description if description else os.path.basename(fileName),
            'formulaLinkbase': formulaLinkBaseString,
            'dateTimeAdded': datetime.now().replace(microsecond=0)
        }
        try:
            self.insertUpdateRssDB(formulaData, 'formulae', action=action, updateCols=None, idCol='formulaId', commit=True, returnStat=False)
            self.cntlr.addToLog(_('Formula id "{}" {}').format(formulaId, action + ('ed' if action=='insert' else 'd')), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
        except Exception as e:
            if self.product == 'postgres':
                pg8000.paramstyle = pgParamStyle
                self.rollback()
            raise e

        if self.product == 'postgres':
            pg8000.paramstyle = pgParamStyle
        res = None
        if returnData:
            res = formulaData
        return res


    def removeFormulaFromDb(self, formulaIds):
        try:
            placeholders = ', '.join(['?'] * len(formulaIds))
            qry = 'DELETE FROM "formulae" WHERE "formulaId" in ({})'.format(placeholders)
            self.execute(qry, params=tuple(formulaIds), fetch=False, commit=True)
            self.addToLog(_('Removed formula(e) with id(s) {}').format(str(formulaIds)), messageCode="RssDB.Info", file=self.conParams.get('database', ''), level=logging.INFO)
        except Exception as e:
            self.rollback()
            self.addToLog(_('Error while removing formula(e) with id(s) {}:\n{}').format(str(formulaIds), str(e)), messageCode="RssDB.Error", file=self.conParams.get('database', ''), level=logging.ERROR)
        return


    def startDBReport(self, host='0.0.0.0', port=None, debug=False, asDaemon=True, fromDate=None, toDate=None, threaded=True):
        return _startDBReport(self, host, port, debug, asDaemon, fromDate, toDate, threaded)


    def checkConnection(self):
        chk = False
        try:
            db = self.conParams['database']
            if self.product == 'postgres':
                chk = self.execute('SELECT current_database();', fetch=True)[0][0] == db
            elif self.product == 'sqlite':
                if db == ':memory:':
                    chk = True
                else:
                    chk = os.path.basename(self.execute('PRAGMA database_list;', fetch=True)[0][2]) == os.path.basename(db)
        except Exception as e:
            pass
        return chk


    def getDbStats(self):
        result = {'textResult': OrderedDict(), 'dictResult':OrderedDict()}
        qry = '''select 'LastUpdate' as description, cast(max("lastUpdate") as text) as val  from "lastUpdate"
            union all
            select 'LatestFiling' as description, cast(max("pubDate") as text) as val  from "filingsInfo"
            union all
            select 'EarliestFiling' as description, cast(min("pubDate") as text) as val  from "filingsInfo"
            union all
            select 'CountFilings' as description, cast(count("filingId") as text) as val from "filingsInfo" where "duplicate"=0
            union all
            select 'LatestFeed' as description, cast(max("feedId") as text) as val from "feedsInfo"
            union all
            select 'EarliestFeed' as description, cast(min("feedId") as text) as val from "feedsInfo"
            union all
            select 'CountFeeds' as description, cast(count("feedId") as text) as val from "feedsInfo"
            union all
            select 'CountFilers' as description, cast(count("cikNumber") as text) as val from "filersInfo"
            union all
            select 'CountFiles' as description, cast(count("fileId") as text) as val from "filesInfo"
            '''

        if self.checkConnection():
            if self.verifyTables(createTables=False):
                stats = self.execute(qry, fetch=True)
                _result = {x[0]:x[1] for x in stats}
                if _result:
                    dbSize = ''
                    if self.product == 'postgres':
                        try:
                            _relsSize = '''SELECT sum(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))
                                            FROM pg_tables 
                                            WHERE schemaname = \'{}\' '''
                            _dbSize = self.execute(_relsSize.format(self.conParams['schema']))[0][0]
                            dbSize = convert_size(_dbSize, 'GB')[2]
                        except Exception as e:
                            self.rollback()
                    elif self.product == 'sqlite':
                        try:
                            _dbfile = self.conParams['database']
                            if os.path.isfile(_dbfile):
                                _dbSize = os.path.getsize(_dbfile)
                                dbSize = convert_size(_dbSize, 'GB')[2]
                        except:
                            conn.rollback()

                    _result['DatabaseSize'] = dbSize
                    
                    result['dictResult'] = _result
                    timeSinceLastUpdate = 'Never Updated'
                    if parser.parse(_result['LastUpdate']).year == 1970:
                        _result['LastUpdate'] = None
                        timeSinceLastUpdate = 'Never Updated'
                    else:
                        td  = parser.parse(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))  - parser.parse(_result['LastUpdate'])
                        days = td.days
                        hours, remainder = divmod(td.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        timeSinceLastUpdate = '{} days, {} hours, {} minutes since last update'.format(days, hours, minutes)
                    result['textResult'] = OrderedDict([
                        ('LastUpdate', str(_result['LastUpdate']) +  ' - ('+timeSinceLastUpdate+')' if _result['LatestFiling'] else 'No Data'),
                        ('CountFeeds', _result['CountFeeds']),
                        ('LatestFeed', str(_result['LatestFeed'])[:4] + '-' + str(_result['LatestFeed'])[-2:] if _result['LatestFeed'] else 'No Data'),
                        ('EarliestFeed', str(_result['EarliestFeed'])[:4] + '-' + str(_result['EarliestFeed'])[-2:] if _result['EarliestFeed'] else 'No Data'),
                        ('CountFilings', _result['CountFilings']),
                        ('LatestFiling', str(_result['LatestFiling']) if _result['LatestFiling'] else 'No Data'),
                        ('EarliestFiling', str(_result['EarliestFiling']) if _result['EarliestFiling'] else 'No Data'),
                        ('CountFiles', str(_result['CountFiles']) if _result['CountFiles'] else 'No Data'),
                        ('CountFilers', _result['CountFilers']),
                        ('DatabaseSize', _result['DatabaseSize'])
                    ])                    
            else:
                result['textResult'] = {'missingTables': ', '.join(set(rssTables) - self.tablesInDB())}
        else:
            result['textResult'] = {'noConnection': 'Could not connect to database'}

        return result
        

    def getReportData(self, fromDate=None, toDate=None):
        '''Get summaries used in db report'''
        if not self.verifyTables(createTables=False):
            return False
        # validate Dates
        for k,v in {'From': fromDate, 'To': toDate}.items():
            if v:
                try:
                    datetime.strptime(v, '%Y-%m-%d')
                except:
                    self.cntlr.addToLog(_('{} Date is not in the correct fromat, date should be in the format yyyy-mm-dd').format(k),
                                        messageCode="RssDB.Error", file=self.conParams.get('database', ''), level=logging.ERROR)
                    return
        
        if (fromDate and toDate) and (datetime.strptime(toDate, '%Y-%m-%d') <= datetime.strptime(fromDate, '%Y-%m-%d')):
            self.cntlr.addToLog(_('To Date must be later than From date'),
                                    messageCode="RssDB.Error", file=self.conParams.get('database', ''), level=logging.ERROR)
            return
        dbStats = self.getDbStats()['dictResult']
        if not fromDate and not toDate:
            lastFiling = dbStats.get('LatestFiling', None)
            if lastFiling:
                lastFilingYear = parser.parse(lastFiling).date().year
                fromDate = str(date(lastFilingYear-2, 1, 1))

        qFromDate = 'and "filingDate">=\'{}\''.format(str(fromDate)) if fromDate else ''
        qToDate = 'and "filingDate"<=\'{}\''.format(str(toDate)) if toDate else ''


        # filings summary query
        sql1 = '''
        with x as (
        select a."cikNumber", b."conformedName", a."feedId", a."formType", a."assignedSic", a."inlineXBRL", count(a."filingId") as "count" 
        from "filingsInfo" a 
        left join "filersInfo" b on a."cikNumber" = b."cikNumber"
        where a."duplicate" = 0 {} {}
        group by a."cikNumber", b."conformedName", a."feedId", a."formType", a."assignedSic", a."inlineXBRL" order by a."feedId" desc)
        select x.*, c."feedMonth" from x left join "feedsInfo" c on x."feedId"=c."feedId"
        '''.format(qFromDate, qToDate)

        # filers' locations
        sql2 = '''select a."cikNumber", a."conformedName", b.* 
                  from "filersInfo" a left join "locations" b on lower(a."businessState") = lower(b."code")'''

        q1 = self.execute(sql1, fetch=True, close=False)
        cols1 = [x[0].decode() if isinstance(x[0], bytes) else x[0] for x in self.cursor.description]
        filingsDataDict = [dict(zip(cols1, x)) for x in q1]

        with open(os.path.join(pathToSQL,'mongodbIndustryClassification.json'), 'r') as industries:
            industry = json.load(industries)

        res_industry = dict()
        for a in industry['industry']:
            if a['industry_classification'] == 'SEC':
                res_industry[str(a['industry_code'])] = {'division_name': a['ancestors'][0]['industry_description'] if  a['ancestors'] else 0}

        q2 = self.execute(sql2, fetch=True, close=False)
        cols2 = [x[0].decode() if isinstance(x[0], bytes) else x[0] for x in self.cursor.description]
        locationsDict = [dict(zip(cols2, x)) for x in q2]

        return dbStats, filingsDataDict, res_industry, locationsDict


    def showStatus(self, msg, clearAfter=2000, end='\n'):
        if self.cntlr is not None:
            if 'end' in self.cntlr.showStatus.__code__.co_varnames:
                self.cntlr.showStatus(msg, clearAfter, end=end)
            elif isinstance(self.cntlr, CntlrCmdLine):
                print(msg, end=end)
            else:
                self.cntlr.showStatus(msg, clearAfter)
        return


    def addToLog(self, msg, **kwargs):
        if self.cntlr is not None:
            self.cntlr.addToLog(msg, **kwargs)
        return


    def changeSchema(self, schema, createIfNotExist=True):
        if self.product in ['postgres']:
            self.schema = schema
            if createIfNotExist:
                _schemas = self.execute('select schema_name from information_schema.schemata;', fetch=True)
                schemas = [s[0] for s in _schemas]
                if not schema in schemas:
                    stat = _('Creating Schema {}').format(schema)
                    self.showStatus(stat)
                    self.execute('CREATE SCHEMA IF NOT EXISTS "{}";'.format(
                        schema), action=stat, fetch=False, commit=True)
            self.execute('SET search_path = "{}";'.format(schema), fetch=False)
            self.showStatus(_('Path set to {}').format(schema))
        return
            

    def tablesInDB(self):
        return set(tableRow[0]
                   for tableRow in 
                   self.execute({"postgres":"SELECT tablename FROM pg_tables WHERE schemaname = '{}';".format(self.schema),
                                 "mysql": "SHOW tables;",
                                 "mssql": "SELECT name FROM sys.TABLES;",
                                 "orcl": "SELECT table_name FROM user_tables",
                                 "sqlite": "SELECT name FROM sqlite_master WHERE type='table';"
                                 }[self.product]))


    def verifyTables(self, createTables=True, dropPriorTables=False, populateFilersInfo=False):
        gettext.install('arelle')
        result = False
        missingTables = set(rssTables) - self.tablesInDB()
        # if no tables, initialize database
        if missingTables == set(rssTables) and createTables:
            self.create([os.path.join(pathToSQL, f) for f in sqlScriptsFiles[self.product]], dropPriorTables=dropPriorTables, populateFilersInfo=populateFilersInfo)
            missingTables = set(rssTables) - self.tablesInDB()
            if missingTables and missingTables != {"sequences"}:
                raise XPDBException("sqlDB:MissingTables",
                                    _("The following tables are missing: %(missingTableNames)s"),
                                    missingTableNames=', '.join(t for t in sorted(missingTables)))
            result = True
        elif missingTables == set(rssTables) and not createTables:
           self.addToLog(_("The following tables are missing: {}").format(', '.join(t for t in sorted(missingTables))),
                            messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        elif not missingTables:
            result = True
        return result
   
    
    def create(self, ddlFiles, dropPriorTables=True, populateFilersInfo=True): # ddl Files may be a sequence (or not) of file names, glob wildcards ok, relative ok
        gettext.install('arelle')
        if dropPriorTables:
            # drop tables
            startedAt = time.time()
            self.showStatus(_("Dropping prior tables"))
            for table in self.tablesInDB():
                result = self.execute('DROP TABLE IF EXISTS %s' % self.dbTableName(table),
                                      close=False, commit=False, fetch=False, action="dropping table")
            self.showStatus(_("Dropping prior sequences"))
            for sequence in self.sequencesInDB():
                result = self.execute('DROP SEQUENCE IF EXISTS %s' % sequence,
                                      close=False, commit=False, fetch=False, action="dropping sequence")
            self.modelXbrl.profileStat(_("XbrlPublicDB: drop prior tables"), time.time() - startedAt)
                    
        startedAt = time.time()
        # process ddlFiles to make absolute and de-globbed
        _ddlFiles = []
        for ddlFile in flattenSequence(ddlFiles):
            if not os.path.isabs(ddlFile):
                # ddlFile = os.path.join(os.path.dirname(__file__), ddlFile)
                raise Exception('Could not find {}'.format(ddlFile))
            for _ddlFile in glob.glob(ddlFile):
                _ddlFiles.append(_ddlFile)
        for ddlFile in _ddlFiles:
            with io.open(ddlFile, 'rt', encoding='utf-8') as fh:
                sql = fh.read().replace('%', '%%')
            
            # SQL server complains about 'GO' statement (SSMS artifact)
            if self.product == 'mssql':
                sql = sql.replace('\nGO\n', '\n')
                
            # pymysql complains about 'DELIMITER' 
            # https://github.com/PyMySQL/mysqlclient-python/issues/64#issuecomment-160226330
            # The next few lines try to remove 'DELIMITER' and the assigned delimiters from ddl
            if self.product == 'mysql':
                # Get all assigned delimiters
                delimiters = [x.strip() for x in re.findall('DELIMITER(.*)', sql)]
                # Exclude default delimiter to be used later in determining statement end
                delimitersExclude = set([x for x in delimiters if not ';' in x])
                # build regex to detect delimiter instruction
                delimiter = '|'.join(set(delimiters))
                subRegex = re.compile(r'\nDELIMITER\s+('+ delimiter + ')|' + '|'.join(delimitersExclude))
                # Remove delimiter instruction and actual assigned delimiter from ddl
                sql = subRegex.sub('', sql)
            # separate dollar-quoted bodies and statement lines
            sqlstatements = []
            def findstatements(start, end, laststatement):

                # Do not terminate statement with ";" if within BEGIN END BLOCK
                beginEndBlocks_ = 0  # Tracks BEGIN END blocks
                sqlLines_ = sql[start:end].split('\n')
                for line in sqlLines_:
                    # Account for blocks and nested blocks
                    if re.search(r'\bBEGIN\b', line.strip(), re.IGNORECASE):
                        beginEndBlocks_ += 1
                    if re.search(r'\bEND\b', line.strip(), re.IGNORECASE) and beginEndBlocks_ > 0:
                        beginEndBlocks_ -= 1
                    stmt, comment1, comment2 = line.partition("--")
                    laststatement += stmt + '\n'
                    if not beginEndBlocks_ and ';' in stmt:
                        if self.product == 'orcl':
                            # cx_Oracle complains about the "/" and ";" at end of statements
                            # in case of triggers => ";"  is needed for oracle to compile the trigger
                            if re.search('CREATE TRIGGER', laststatement, re.IGNORECASE):
                                laststatement = re.sub('^/', '', laststatement)
                            else:
                                # Otherwise both "/" and ";" are removed
                                laststatement = re.sub('^/|;$', '', laststatement)
                        sqlstatements.append(laststatement)
                        laststatement = ''
                return laststatement
            stmt = ''
            i = 0
            patternDollarEsc = re.compile(r"([$]\w*[$])", re.DOTALL + re.MULTILINE)
            while i < len(sql):  # preserve $$ function body escaping
                match = patternDollarEsc.search(sql, i)
                if not match:
                    stmt = findstatements(i, len(sql), stmt)
                    sqlstatements.append(stmt)
                    break
                # found match
                dollarescape = match.group()
                j = match.end()
                stmt = findstatements(i, j, stmt)  # accumulate statements before match
                i = sql.find(dollarescape, j)
                if i > j: # found end of match
                    if self.product == "mysql":
                        # mysql doesn't want DELIMITER over the interface
                        stmt = sql[j:i]
                        i += len(dollarescape)
                    else:
                        # postgres and others want the delimiter in the sql sent
                        i += len(dollarescape)
                        stmt += sql[j:i]
                    sqlstatements.append(stmt)
                    # problem with driver and $$ statements, skip them (for now)
                    stmt = ''
            if self.product in ['postgres']:
                stat = _('Creating Schema {}').format(self.schema)
                self.showStatus(stat)
                self.execute('CREATE SCHEMA IF NOT EXISTS "{}";'.format(
                    self.schema), action=stat, fetch=False)
            action = "executing ddl in {}".format(os.path.basename(ddlFile))
            for i, sql in enumerate(sqlstatements):
                if any(cmd in sql
                       for cmd in ('CREATE TABLE', 'CREATE SEQUENCE', 'INSERT INTO', 'CREATE TYPE',
                                   'CREATE FUNCTION', 
                                   # comma after 'DROP' and add 'CREATE TRIGGER'
                                   'DROP', 'CREATE TRIGGER',
                                   'SET',
                                   'CREATE INDEX', 'CREATE UNIQUE INDEX', # 'ALTER TABLE ONLY'
                                   'CREATE VIEW', 'CREATE OR REPLACE VIEW', 'CREATE MATERIALIZED VIEW'
                                   )):
                    statusMsg, sep, rest = sql.strip().partition('\n')
                    self.showStatus(statusMsg[0:50])
                    if self.product == 'postgres' and 'create function' in sql.lower():
                        sql = sql.replace('%%', '%')
                    result = self.execute(sql, close=False, commit=False, fetch=False, action=action)
                    if TRACESQLFILE:
                        with io.open(TRACESQLFILE, "a", encoding='utf-8') as fh:
                            fh.write("\n\n>>> ddl {0}: \n{1} \n\n>>> result: \n{2}\n"
                                     .format(i, sql, result))
                            fh.write(sql)
        
        updateCikTickerMapping(self)
        if populateFilersInfo:
            _populateFilersInfo(self)
        self.showStatus("")
        self.conn.commit()
        self.modelXbrl.profileStat(_("XbrlPublicDB: create tables"), time.time() - startedAt)
        self.closeCursor()
        return


    def insertUpdateRssDB(self, inputData, dbTable, action='insert', updateCols=None, idCol=None, commit=False, returnStat=False):
        '''action either `insert` or `update` '''
        self.verifyTables(createTables=True)
        if self.product == 'postgres':
            pgParamStyle = pg8000.paramstyle
            pg8000.paramstyle = 'named'

        _tbl = dbTable
        colTypeFunc = self.columnTypeFunctions(dbTable)
        _cols =  chkToList(updateCols, str) if action=='update' and updateCols else rssCols[_tbl]
        columns = ', '.join(['"' + x + '"' for x in _cols])
        placeholders = ":" + ', :'.join([x + colTypeFunc[x][0] if self.product=='postgres' else x for x in _cols])
        _placeholders = ('SELECT ' + placeholders,)[0] if len(_cols)== 1 else placeholders
        _action = action
        _idCol = chkToList(idCol if idCol else rssCols[_tbl][0], str)
        updateKeys = ' AND '.join(['"{0}"=:{0}'.format(col) for col in _idCol])

        _sql = {"insert": 'INSERT INTO "{}" ({}) VALUES ({})'.format(_tbl, columns, placeholders),
                    "update": 'UPDATE "{}" SET ({}) = ({}) WHERE {}'.format(_tbl, columns, _placeholders, updateKeys)
                    }[_action]
        msg = {'update':_("Updating {}"), 'insert': _('Inserting into {}')}[_action].format(_tbl)
        startInsertTime = time.perf_counter()
        _inputData = inputData if isinstance(inputData, list) else [inputData]
        actionMsg = ''
        _stats = None
        if len(_inputData) > 0:
            try:
                self.showStatus(msg)
                cur = self.cursor
                cur.executemany(_sql, _inputData)
                actionMsg = _('{} {} row(s) in {}').format(_action + ('ed' if _action=='insert' else 'd',)[0], cur.rowcount, _tbl)
                self.showStatus(actionMsg)
            except Exception as e:
                self.rollback()
                raise e
        if commit:
            self.commit()
        if self.product == 'postgres':
            pg8000.paramstyle = pgParamStyle
        self.addToLog(_("Finished {} in {} secs").format(msg,
                round(time.perf_counter() - startInsertTime, 3)),
                messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        result = None
        if returnStat:
            result = {action: cur.rowcount}
        return result


    def xdoAll(self, loc=None, last=None, dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, 
                maxWorkers=None, updateDB=True, reloadCache=False, updateExisting=True, refreshAll=False, 
                timeOut=3, retries=3, includeLatest=False, getFiles=True, getXML=False, getFilers=True, updateTickers=True, q=None):
        '''Creates and populates rssDB'''
        return _doAll(self, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, getRssItems=getRssItems, returnInfo=returnInfo,
                     maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, refreshAll=refreshAll,
                     timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML, 
                     getFilers=getFilers, updateTickers=updateTickers, q=q)

    
    def doAll(self, setAutoUpdate=False, waitFor=timedelta(minutes=wait_duration), duration=timedelta(hours=1), loc=None, last=None, 
                dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, maxWorkers=None, updateDB=True, reloadCache=False, 
                updateExisting=True, refreshAll=False, timeOut=3, retries=3, includeLatest=True, getFiles=True, getXML=False, 
                getFilers=True, updateTickers=True, q=None):
        '''Creates and populates rssDB'''
        return _doAll(self, setAutoUpdate=setAutoUpdate, waitFor=waitFor, duration=duration, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, 
                            getRssItems=getRssItems, returnInfo=returnInfo, maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, 
                            refreshAll=refreshAll, timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML,
                            getFilers=getFilers, updateTickers=updateTickers, q=q)


    def updateRssFeeds(self, loc=None, getRssItems=False, updateDB=False, maxWorkers=None, returnInfo=False,
                        dateFrom=None, dateTo=None, last=None, reloadCache=False, includeLatest=False, getFiles=True, getXML=False, q=None):
        return _updateRssFeeds(self, loc=loc, getRssItems=getRssItems, updateDB=updateDB, maxWorkers=maxWorkers, returnInfo=returnInfo,
                        dateFrom=dateFrom, dateTo=dateTo, last=last, reloadCache=reloadCache, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML, q=q)


    def updateFilersInfo(self, updateExisting=False, refreshAll=False, updateDB=False, 
                            maxWorkers=None, timeOut=3, retries=3, returnData=False):
        sql_new = '''
        SELECT distinct "cikNumber"
        FROM   "filingsInfo" a
        WHERE  NOT EXISTS (
        SELECT  "cikNumber"
        FROM   "filersInfo"
        WHERE  "cikNumber" = a."cikNumber"
        );
        '''
        sql_update = '''
        select * from
        (select c."cikNumber", c."companyName", c."conformedName",
                c."formerNames",  c."pubDate", 
                (lower(c."companyName") = lower(c."conformedName")) as test
        from
        (
            select a."cikNumber", a."companyName", b."conformedName", b."formerNames", a."pubDate"
            from
                (select t1."companyName", t1."cikNumber", t1."pubDate"
                from "filingsInfo" t1
                inner join
                (
                select max("filingId") maxId, "cikNumber"
                from "filingsInfo"
                group by "cikNumber"
                ) t2
                on t1."cikNumber" = t2."cikNumber"
                and t1."filingId" = t2.maxId) a
            inner join "filersInfo" b
            on a."cikNumber" = b."cikNumber"
            order by a."cikNumber"
            ) c
        ) d
        where test = false;
        '''
        hasTables = rssTables[3] in self.tablesInDB()
        startTime = time.perf_counter()
        newCiksData = []
        updatedExistingCiksData = []
        if refreshAll and hasTables:
            self.execute('Delete From "{}"'.format(rssTables[3]), fetch=False)
            
        _newCiks = []
        _newCiksList = []
        allNewCikData = 0
        if hasTables:
            self.showStatus(_('Checking for new CIKs'))
            _newCiks = self.execute(sql_new, fetch=True)
        # patch retrive and insert ciks        
        if len(_newCiks)>0:
            _newCiks_patches = [_newCiks[i:i + 100] for i in range(0, len(_newCiks), 100)]
            indx = 1
            for nP, patch in enumerate(_newCiks_patches):
                self.showStatus(_('Retriving new CIKs patch {} of {}').format(nP + 1, len(_newCiks_patches)))
                _newCiksList = [x[0] for x in patch]
                _newCiksData = getFilerInformation(self, _newCiksList, timeOut=timeOut, i=indx, _all=len(_newCiks),  maxWorkers=maxWorkers, retries=retries)
                newCiksData = [x['filerInfo'] for x in _newCiksData['retrived']]
                indx = _newCiksData.get('i')
                if updateDB:
                    if len(newCiksData)>0:
                        self.showStatus(_('Inserting new ciks patch {} of {}').format(nP+1, len(_newCiks_patches)))
                        try:
                            self.insertUpdateRssDB(newCiksData, rssTables[3], action='insert')
                            self.commit()
                            allNewCikData += len(newCiksData)
                        except Exception as e:
                            self.rollback()
                            raise e
        allUpdatedExistingCiksData = 0
        if updateExisting and hasTables:
            self.showStatus(_('Checking for CIKs with changes'))
            _existingCiks = self.execute(sql_update, fetch=True, close=False)
            cols_chk = [c[0].decode() if isinstance(c[0], bytes) else c[0] for c in self.cursor.description]
            chk_dict = [dict(zip(cols_chk, x)) for x in _existingCiks]
            to_refresh = []
            for d in chk_dict:
                # make sure former names are parsed, and in revese order (latest name change in [0] position)
                formerNames = d['formerNames'] if isinstance(d['formerNames'], list) else json.loads(d['formerNames'] if d['formerNames'] else '[]')
                formerNames.sort(key=lambda x: parser.parse(x['date']) if isinstance(x['date'], str) else x['date'], reverse=True)
                # no former names but still conformed name != company name, then refresh information
                formerNameDate = parser.parse(formerNames[0]['date']).date() if len(formerNames) > 0 else date.min
                # sql_update returns ciks that has different company name in filerInfo and last filing
                # when pubDate of a filing is later than last name change then propably filer information 
                # needs to be refresh, not the most accurate test but can hint changes in filer's info
                pubDate = parser.parse(d['pubDate']).date() if isinstance(d['pubDate'], str) else d['pubDate'].date()
                if pubDate > formerNameDate:
                    to_refresh.append(d['cikNumber'])

            if len(to_refresh) > 0:
                to_refresh_patches = [to_refresh[i:i + 100] for i in range(0, len(to_refresh), 100)]
                indx2 = 1
                for nP2, patch2 in enumerate(to_refresh_patches):
                    self.showStatus(_('Retriving CIKs with changes patch {} of {}').format(nP2 + 1, len(to_refresh_patches)))
                    _updatedExistingCiksData = getFilerInformation(self, patch2, timeOut=timeOut, maxWorkers=maxWorkers, i=indx2, _all=len(to_refresh), retries=retries) 
                    updatedExistingCiksData = [x['filerInfo'] for x in _updatedExistingCiksData['retrived']]
                    indx2 = _updatedExistingCiksData.get('i')
                    if updateDB:
                        if len(updatedExistingCiksData) > 0:
                            self.showStatus(_('Updating ciks patch {} of {}').format(nP2 + 1, len(to_refresh_patches)))
                            try:
                                self.insertUpdateRssDB(updatedExistingCiksData, rssTables[3], action='update')
                                allUpdatedExistingCiksData += len(updatedExistingCiksData)
                                self.commit()
                            except Exception as e:
                                self.rollback()
                                raise e       
        endTime = time.perf_counter()
        _msg = _('Finished updating filers information in {} secs').format(round(endTime-startTime,3))
        self.addToLog(_msg, messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        # result = {'summary':{'filersInfo':{'insert': len(newCiksData), 'update': len(updatedExistingCiksData)}}, 'stats': _msg}
        result = {'summary':{'filersInfo':{'insert': allNewCikData, 'update': allUpdatedExistingCiksData}}, 'stats': _msg}
        if returnData:
            result['newCiks'] = newCiksData
            result['updatedCiks'] = updatedExistingCiksData
            
        return result


    def getMonthlyFeedsLinks(self, loc=None, maxWorkers=None, dateFrom=None, dateTo=None, last=None):
        """Returns Monthly feeds Links  that are not in DB or with lastModified date later than in DB"""
        
        feeds, compareCol, startTime = _getMonthlyFeedsLinks(self, loc=loc, maxWorkers=maxWorkers, dateFrom=dateFrom, dateTo=dateTo, last=last)

        results = []

        if feeds:
            feeds.sort(key=lambda x: x['feedId'])
            if any((dateFrom, dateTo)):
                _from = parser.parse(dateFrom) if dateFrom else datetime.min
                _to = parser.parse(dateTo) if dateTo else datetime.max
                if _to <= _from:
                    raise Exception('dateTo must be later than dateFrom')
                _feeds = [x for x in feeds if x['feedDate'] >= _from and x['feedDate'] <= _to]
                feeds = _feeds
            if last:
                _feeds = feeds[-last:]
                feeds = _feeds
            existing = []
            existingIds = []
            if all([x in self.tablesInDB() for x in rssTables[:2]]):
                self.showStatus(_('Getting existing feeds information'), 2000)
                _qry = 'SELECT "feedId", "{}" from "{}"'.format(compareCol, rssTables[0])
                _existing = self.execute(_qry, close=False)
                existing = [(x[0], parser.parse(x[1],  tzinfos={'EST':'UTC-5:00', 'EDT':'UTC-4:00'}) if isinstance(
                    x[1], str) else x[1]) for x in _existing]
                existingIds = [i[0] for i in existing]
            else:
                self.showStatus(_('Rss DB tables not intialized, returning all available feeds'))
            for x in feeds:
                if x['feedId'] not in existingIds:
                    x['isNew'] = True
                    results.append(x)
                    self.addToLog(_('New: {}').format(x['link']), messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
                # first check mod date to avoid downloading and parsing feeds with no new entries
                elif existing[existingIds.index(x['feedId'])][1]:
                    if x[compareCol] > existing[existingIds.index(x['feedId'])][1]:
                        x['isNew'] = False
                        results.append(x)
                        self.addToLog(_('Updatable: {}').format(x['link']), messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
                # finally just download the and parse the document and compare it to existing
                else:
                    x['isNew'] = False
                    results.append(x)
                    self.addToLog(_('May need update: {}').format(x['link']), messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
                # leaves out existing ids with compareCol not later than new lastModifiedDate\lastBuildDate
        endTime = time.perf_counter()
        self.addToLog(_('Finished getting SEC monthly XBRL RSS Feed links in {} sec(s)').format(round(
            endTime-startTime, 3)), messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        return results


    def getFeedInfo(self, link, lastModifiedDate, isNew, reloadCache=False, getFiles=True, getXML=False):
        """Gets feed info ready to insert in db"""
        startAllTime = time.perf_counter()
        feedInfo, _rssItemsList = _getFeedInfo(self, link, lastModifiedDate, isNew, reloadCache)
        f_id = int(str(feedInfo['feedId']) + '100000' ) + 1
        if not isNew and rssTables[1] in self.tablesInDB():
            _qry = '''select "feedId", max("filingId")
                        FROM "{}"
                        WHERE "feedId"={}
                        GROUP BY "feedId";'''.format(rssTables[1], feedInfo['feedId'])
            try:
                max_id_qry = self.execute(_qry, close=False)
                if max_id_qry:
                    max_filings_id = max_id_qry[0][1]
                    f_id = max_filings_id + 1
            except Exception as e:
                self.rollback()
                raise e
        result = { x:[] for x in rssTables}
        for rssI in _rssItemsList:
            itemInfo = getRssItemInfo(rssI, feedInfo['feedId'], f_id, getFiles, getXML)
            for _k in itemInfo.keys():
                result[_k].append(itemInfo[_k]) 
            f_id +=1
        result[rssTables[0]] = feedInfo
        result['isNew'] = isNew
        _msg = _("Finished extracting data from {} in {} secs").format(link, round(time.perf_counter() - startAllTime, 3))
        try:
            logs = self.cntlr.logHandler.getLines()
        except:
            pass
        self.cntlr.modelManager.modelXbrl.close()
        self.cntlr.modelManager.close()
        gc.collect()
        self.addToLog(_msg, messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        return result


    def getExistingFeeds(self):
        feedsIds = []
        if rssTables[0] in self.tablesInDB():
            try:
                _feedsIds = self.execute('SELECT "{}" from "{}"'.format(rssCols[rssTables[0]][0], rssTables[0]), fetch= True)
                feedsIds = [x[0] for x in _feedsIds]
            except Exception as e:
                self.rollback()
                raise e
        return feedsIds


    def updateDuplicateFilings(self, commit=True):
        statTime = time.perf_counter()
        dups = [{'filingId':x[0], 'duplicate': 1} for x in self.execute('SELECT * FROM v_duplicate_filings', fetch=True)]
        stat_filings = {'update':0}
        stat_files = {'update':0}
        if len(dups):
            try:
                stat_filings = self.insertUpdateRssDB(dups, 'filingsInfo', 'update', updateCols='duplicate', idCol='filingId', commit=True, returnStat=True)
                stat_files = self.insertUpdateRssDB(dups, 'filesInfo', 'update', updateCols='duplicate', idCol='filingId', commit=True, returnStat=True)
            except Exception as e:
                self.rollback()
                raise e
        else:
            self.addToLog(_('No duplicate Filings to tag'), messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        endTime = round(time.perf_counter() - statTime, 3)

        msg = _('Finished updating filing duplicates in {} sec(s)').format(endTime)
        stat = {'filingsInfo': stat_filings, 'filesInfo': stat_files}
        self.addToLog(msg, messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        stat['msg'] = msg
        return stat


    def getById(self, idsList, tableName, idCol=None,  idDataType=int, returnCols=None, additionalWhereClauseString=None):
        '''Get rows by ids from specified tables with optional where clause'''
        result = None
        if not idsList:
            raise Exception('No ids to get')
        
        joiner = lambda x: "'" + str(x) + "'" if not idDataType == int else str(x)
        _idsList = ', '.join([joiner(x) for x in chkToList(idsList, idDataType)])
        _returnCols = '*'
        if returnCols:
            _returnCols = ', '.join(['"' + x + '"' for x in returnCols])
        
        if not additionalWhereClauseString:
            additionalWhereClauseString = ''
        
        if not idCol:
            idCol = rssCols[tableName][0]
        
        qry = 'SELECT {a} FROM "{b}" WHERE "{c}" in ({d}) {e}'.format(a=_returnCols, b=tableName, c=idCol, d=_idsList, e=additionalWhereClauseString)

        try:
            qryResult = self.execute(qry, fetch=True, close=False)
            colNames = [x[0].decode() if isinstance(x[0], bytes) else x[0] for x in self.cursor.description]
            result = [dict(x) for x in [zip(colNames, y) for y in qryResult]]
        except Exception as e:
            self.rollback()
            raise e

        return result

    def searchFilings(self, companyName=None, tickerSymbol=None, cikNumber=None, formType=None, 
                        assignedSic=None, dateFrom=None, dateTo=None, inlineXBRL=None, 
                        limit=100, getFiles=False, filingIds=None, **kwargs):
        # accommodate both list and string input
        qry_result = {}
        params = None
        if not filingIds: # shortcut
            companyName = ','.join(companyName) if isinstance(companyName, (list, tuple, set)) else companyName
            tickerSymbol = ','.join(tickerSymbol) if isinstance(tickerSymbol, (list, tuple, set)) else tickerSymbol
            cikNumber = ','.join(cikNumber) if isinstance(cikNumber, (list, tuple, set)) else cikNumber
            formType = ','.join(formType) if isinstance(formType, (list, tuple, set)) else formType
            assignedSic = ','.join([str(x) for x in assignedSic]) if isinstance(assignedSic, (list, tuple, set)) else assignedSic
            inlineFilter = {
                'yes': '1',
                'no': '0'
            }
            whereClause = OrderedDict([
                ('companyName', ['%' + x.strip() + '%' for x in companyName.split(',')] if companyName else []),
                ('tickerSymbol', [x.strip() for x in tickerSymbol.split(',')] if tickerSymbol else []),
                ('cikNumber', [x.strip() for x in cikNumber.split(',')] if cikNumber else []),
                ('formType', ['%' + x.strip() + '%' for x in formType.split(',')] if formType else []),  
                ('assignedSic', [x.strip() for x in assignedSic.split(',')] if assignedSic else []), 
                ('dateFrom', [dateFrom] if dateFrom else []),
                ('dateTo', [dateTo] if dateTo else []),
                ('inlineXBRL', [str(inlineFilter[inlineXBRL.lower()])] if inlineXBRL else []),
                ('limit', [limit] if limit else [100])])

            whereClausePlaceHolders = ' AND '.join(filter(None, [
                '(' + ' OR '.join(filter(None, [
                    ' OR '.join(['a."companyName" LIKE ?' for n in whereClause['companyName']]
                                ) if whereClause['companyName'] else None,
                    'b."tickerSymbol" IN ({})'.format(', '.join(
                        '?' * len(whereClause['tickerSymbol']))) if whereClause['tickerSymbol'] else None,
                    'a."cikNumber" IN ({})'.format(', '.join(
                        '?' * len(whereClause['cikNumber']))) if whereClause['cikNumber'] else None
                ])) + ')' if any([whereClause['companyName'], whereClause['tickerSymbol'], whereClause['cikNumber']]) else None,
                '(' + ' OR '.join(['a."formType" LIKE ?' for n in whereClause['formType']]
                                ) + ')' if whereClause['formType'] else None,
                'a."assignedSic" IN ({})'.format(', '.join(
                    '?' * len(whereClause['assignedSic']))) if whereClause['assignedSic'] else None,
                'a."filingDate" >= ?' if whereClause['dateFrom'] else None, 
                'a."filingDate" <= ?' if whereClause['dateTo'] else None,
                'a."inlineXBRL" = ?' if whereClause['inlineXBRL'] else None,
            ]))

            params = tuple(filter(None,([i for x in whereClause.values() for i in x])))

            qry='''
            SELECT * 
            FROM "filingsInfo" a
                {}
            {} {}
            ORDER BY "filingId" DESC
            LIMIT ?
            '''.format('LEFT JOIN "cikTickerMapping" b on a."cikNumber" = b."cikNumber"' if tickerSymbol else '', 'WHERE' if whereClausePlaceHolders else '', whereClausePlaceHolders)

        else:
            filingIds = ','.join([str(x) for x in filingIds]) if isinstance(filingIds, (list, tuple, set)) else filingIds
            qry = f'SELECT * FROM "filingsInfo" WHERE "filingId" in ({filingIds})'

        self.showStatus(_('Retriving Data'))
        try:
            if self.product == 'postgres':
                paraStyle = pg8000.paramstyle
                pg8000.paramstyle = 'qmark'
                qry = qry.replace(' LIKE ', ' ILIKE ' )
            qry_result = self.execute(qry, params=params, close=False)
        except Exception as e:
            self.rollback()
            if self.product == 'postgres':
                pg8000.paramstyle = paraStyle
            raise e

        resultDict = dict(filings=[], files=[])

        _cols = [x[0] for x in self.cursor.description]
        cols = [x.decode() if isinstance(x, bytes) else x for x in _cols]
        resultDict['filings'] = [dict(zip(cols, x)) for x in qry_result]

        if getFiles and qry_result:
            filings_ids = tuple(x['filingId'] for x in resultDict['filings'])
            qry_files = 'SELECT * From "filesInfo" WHERE "filingId" IN ({})'.format(', '.join(['?']*len(filings_ids)))
            try:
                # qry_result_files = self.execute(qry_files, params= filings_ids, close=False)
                _qry_string = f'SELECT * From "filesInfo" WHERE "filingId" IN ({",".join([str(x) for x in filings_ids])})'
                qry_result_files = self.execute(_qry_string, close=False)
            except Exception as e:
                self.rollback()
                if self.product == 'postgres':
                    pg8000.paramstyle = paraStyle
                raise e
            _cols_files = [x[0] for x in self.cursor.description]
            cols_files = [x.decode() if isinstance(x, bytes) else x for x in _cols_files]
            resultDict['files'] = [dict(zip(cols_files, x)) for x in qry_result_files]
        self.addToLog(_('Retrived {} filing(s) and {} file(s)').format(len(resultDict['filings']), len(resultDict['files'])),
                            messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        return resultDict

    def searchFilers(self, companyName=None, tickerSymbol=None, cikNumber=None, industry=None, limit=100, **kwargs):
        # accommodate both list and string input
        companyName = ','.join(companyName) if isinstance(companyName, (list, tuple, set)) else companyName
        tickerSymbol = ','.join(tickerSymbol) if isinstance(tickerSymbol, (list, tuple, set)) else tickerSymbol
        cikNumber = ','.join(cikNumber) if isinstance(cikNumber, (list, tuple, set)) else cikNumber
        industry = ','.join([str(x) for x in industry]) if isinstance(industry, (list, tuple, set)) else industry
        whereClause = OrderedDict([
            ('companyName', ['%' + x.strip() + '%' for x in companyName.split(',')] if companyName else []),
            ('tickerSymbol', [x.strip() for x in tickerSymbol.split(',')] if tickerSymbol else []),
            ('cikNumber', [x.strip() for x in cikNumber.split(',')] if cikNumber else []),
            ('industry', [x.strip() for x in industry.split(',')] if industry else []), 
            ('limit', [limit] if limit else [100])])

        whereClausePlaceHolders = ' AND '.join(filter(None, [
            '(' + ' OR '.join(filter(None, [
                ' OR '.join(['a."conformedName" LIKE ?' for n in whereClause['companyName']]
                            ) if whereClause['companyName'] else None,
                'b."tickerSymbol" IN ({})'.format(', '.join(
                    '?' * len(whereClause['tickerSymbol']))) if whereClause['tickerSymbol'] else None,
                'a."cikNumber" IN ({})'.format(', '.join(
                    '?' * len(whereClause['cikNumber']))) if whereClause['cikNumber'] else None
            ])) + ')' if any([whereClause['companyName'], whereClause['tickerSymbol'], whereClause['cikNumber']]) else None,
            'a."industry_code" IN ({})'.format(', '.join(
                '?' * len(whereClause['industry']))) if whereClause['industry'] else None
        ]))

        params = tuple(filter(None,([i for x in whereClause.values() for i in x])))

        qry='''
        SELECT a.*, b."tickerSymbol" 
        FROM "filersInfo" a
            {}
        {} {}
        LIMIT ?
        '''.format('LEFT JOIN "cikTickerMapping" b on a."cikNumber" = b."cikNumber"', 'WHERE' if whereClausePlaceHolders else '', whereClausePlaceHolders)

        if self.product == 'postgres':
            paraStyle = pg8000.paramstyle
            pg8000.paramstyle = 'qmark'
            qry = qry.replace(' LIKE ', ' ILIKE ' )

        qry_result = {}
        self.showStatus(_('Retriving Data'))
        try:
            qry_result = self.execute(qry, params=params, close=False)
        except Exception as e:
            self.rollback()
            if self.product == 'postgres':
                pg8000.paramstyle = paraStyle
            raise e
        
        resultDict = dict(filers=[])

        _cols = [x[0] for x in self.cursor.description]
        cols = [x.decode() if isinstance(x, bytes) else x for x in _cols]
        filersDicts = [dict(zip(cols, x)) for x in qry_result]

        # make tickers unique
        unique_ciks = {x['cikNumber'] for x in filersDicts}
        unique_filers_dicts = []
        for cik in unique_ciks:
            tickersSet = set()
            # find all tickers
            for t in filersDicts:
                if t['cikNumber'] == cik:
                    if not t['tickerSymbol'] is None:
                        tickersSet.add(t['tickerSymbol'])
            # get 1 filer info
            for d in filersDicts:
                if d['cikNumber'] == cik:
                    d['tickerSymbol'] = '|'.join(tickersSet)
                    unique_filers_dicts.append(d)
                    break
        resultDict['filers'] = unique_filers_dicts

        self.addToLog(_('Retrived {} filer(s) with {} ticker symbol(s)').format(len(unique_filers_dicts), len(filersDicts)),
                            messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
        return resultDict

    def dumpFilersInfo(self):
        """Creates dumps filers table to a pickle file 
        This is useful when creating db as retriving filers information consumes a lot of time because in addition
        to the time consumed in parsing each filer's info page, SEC has a 10 requests per second limit, so the process
        needs to be slowed down to accommodate the request limit.

        When creating db filersInfo table is populated from this dump file, and updated with each update.

        `WARNING` eventhough the update process tries to detect and update outdated filers' information, filers' information
        might still be outdated, every now and then it is good to select 'refreshAll' option when updating the db to retrive ALL
        current filers' information from the SEC site to refresh filersInfo table this is time consuming but useful if done periodically.
        """
        filersTable = self.execute('SELECT * FROM "filersInfo"', fetch=True, close=False)
        cols = [x[0].decode() if isinstance(x[0], bytes) else x[0] for x in self.cursor.description]
        filersInfo = [OrderedDict(zip(cols, x)) for x in filersTable]
        filersInfoDict = {'retrivedOn': datetime.now().replace(microsecond=0), 'sourceDBType':self.product, 'data': filersInfo}
        fileName = os.path.join(pathToSQL, '{}_filersInfo.pkl'.format(self.product))
        self.addToLog(_('Creating/updating filers dump'), messageCode='RssDB.Info', file=self.conParams.get('database', ''), level=logging.INFO)
        with open(fileName, 'wb') as f:
            pickle.dump(filersInfoDict, f)
        return

    def get_existing_filing_numbers(self, form_types:list):
        '''List of filing numbers (accession numbers) of form_types existing in db
        returns cols: 
            filingsInfo.filingId, filingsInfo.accessionNumber, filingsInfo.formType, filingsInfo.acceptanceDatetime
        '''
        qry = f'''SELECT "filingId", "accessionNumber", "formType", "acceptanceDatetime" FROM "filingsInfo"
                WHERE "duplicate"=0'''

        existing_filings = None
        if form_types is not None:
            if isinstance(form_types, (list, tuple, set)):
                form_types = ','.join([self.dbStr(str(x)) for x in form_types])
            else:
                form_types = self.dbStr(form_types)
            qry += f' AND "formType" IN ({form_types})'
        try:
            existing_filings = self.execute(qry, close=False)
            cols = [x[0].decode() if type(x[0]) is bytes else x[0] for x in self.cursor.description]
            existing_filings = [dict(zip(cols, x)) for x in existing_filings]
        except Exception as ex:
            if self.product == 'postgres':
                self.rollback()
            raise
        return existing_filings

class rssMongoDbConnection:
    def __init__(self, cntlr, host, database, user, password, port, timeout, product, schema, createSchema=False, createDB=False):
        self.conParams = {'cntlr': None, 'user': user, 
                            'password': password, 'host': host, 
                            'port': port, 'database': database, 
                            'timeout': timeout, 'product': product, 'schema': schema}
        
        self.autoUpdateSet = False
        self.updateStarted = False   
        self.updateStopped = False     
        # relevant params
        _connParams = {'username': user, 'password': password, 'host': host, 'port': port if port else 27017, 'connectTimeoutMs': timeout*1000 if timeout else 20000}
        # if full connection string entered in host field remove other paramaters (username and password will conflict with host string)
        connParams = {k:v for k,v in _connParams.items() if v}
        self.cntlr = cntlr
        self.mongoClient = MongoClient(**connParams)
        self.dbName = database
        self.product = 'mongodb'
        self.dbConn = None
        if createDB:
            self.dbConn = self.mongoClient[database]
            if not database in self.mongoClient.list_database_names():
                self.create()
        if not createDB:
            if database in self.mongoClient.list_database_names():
                self.dbConn = self.mongoClient[database]
            else:
                self.addToLog(_('Database {} does not exist on {}').format(database, host), messageCode="RssDB.Info", 
                                file=database,  level=logging.INFO)
                raise Exception('Database {} does not exist on {}'.format(database, host))

        chk = self.checkConnection()
        if not chk:
            self.close()
            raise Exception('Could not connet to database {}'.format(database))

    def getFormulae(self):
        res = list(self.dbConn.formulae.find({}, {'_id':0, 'formulaLinkbase':0}))
        return res

    def addFormulaToDb(self, fileName, formulaId=None, description=None, formulaLinkBaseString=None, replaceExistingFormula=False, returnData=True):
        '''Inserts a new or updates existing formula in the database

        Cannot have duplicate fileId OR fileName in db, checks if filename already exists in database, if exists and `replaceExistingFormula` set to False,
        file is not added, if `replaceExistingFormula` set to True, the database entry is updated with the current data. 

        The xbrl formula can be read as string via `formulaLinkBaseString` parameter directly, if only filename is provided the file is read from drive.


        args:
            fileName: path to formula linkbase file on local drive or just a unique name if `formulaLinkBaseString` is used
            description: a brief description of what the function does, defaults to file basename
            formulaLinkBaseString: string of the formula link base
            replaceExisting: if there is an entry with the same file name in the db, and this parameter is set to true, the existing entry is replace with current data    
        '''
        noFile = 'NO_FILE'
        if not any([fileName, formulaLinkBaseString]):
            self.cntlr.addToLog(_('At least one of fileName or formulaLinkbaseString must be entered.'), messageCode="RssDB.Error", file=self.conParams.get('database',''), level=logging.ERROR)
            return
        action = 'insert'
        if fileName is None:
            fileName = noFile
        
        # get new id if id is not chosen
        if not formulaId:
            _ids = [x['formulaId'] for x in self.dbConn.formulae.find({}, {'_id':0, 'formulaId':1})]
            if _ids:
                formulaId = max(_ids) + 1
            else:
                formulaId = 1000

        formulaData = dict()
        _existingFiles = list(self.dbConn.formulae.find({'$or':[{'fileName': fileName}, {'formulaId': formulaId}]}))
        existingFormulaId = [x for x in _existingFiles if x['formulaId']==formulaId]
        existingFileNames = [x for x in _existingFiles if x['fileName']==fileName]

        if existingFormulaId or existingFileNames:
            existingIds = [x['formulaId'] for x in existingFileNames]
            if replaceExistingFormula:
                action = 'update'
                if not formulaId or formulaId not in existingIds:
                    self.cntlr.addToLog(_('Enter an existing formulaId to update, existing ids: {}').format(str(existingIds)), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
                    return
            else:
                idMsg = 'formula with formulaId {}'.format(str(formulaId)) if len(existingFormulaId) else ''
                fNameMsg = '{} formula(e) with fileName {} and formulaId(s) {}'.format(str(len(existingFileNames)), fileName, str(existingIds)) if len(existingFileNames) else ''
                self.cntlr.addToLog(_('DB has already {}{}{}, set "replaceExistingFormula" to True and enter a formulaId to update an existing formula by Id').format(
                    idMsg, ' and ' if idMsg and fNameMsg else '', fNameMsg), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
                return

        if action == 'insert':
            # get new id if id is not chosen
            if not formulaId:
                _ids = list(self.dbConn.formulae.aggregate([{'$group':{'_id':None,'maxId':{'$max':'$formulaId'}}},{'$project':{'maxId':'$maxId','_id':0}}]))
                if _ids:
                    formulaId = max([x['maxId'] for x in _ids]) + 1
                else:
                    formulaId = 1000

        lb = ''
        if formulaLinkBaseString:
            lb = formulaLinkBaseString
        elif os.path.isfile(fileName):
            with open(fileName, 'r') as fp:
                lb = fp.read().replace('\n', '')

        if lb:
            lb_xml = etree.fromstring(lb).getroottree()
            lb_string = etree.tostring(lb_xml) #, pretty_print=True, encoding=lb_xml.docinfo.encoding if lb_xml.docinfo.encoding else None
            formulaLinkBaseString = lb_string
        else:
            raise Exception('No valid file path or formula linkbase string was provided')
                
        formulaData = {
            'formulaId': formulaId,
            'fileName': fileName,
            'description': description if description else os.path.basename(fileName),
            'formulaLinkbase': formulaLinkBaseString,
            'dateTimeAdded': datetime.now().replace(microsecond=0)
        }
        

        self.insertUpdateRssDB(formulaData, 'formulae', action=action, updateFields=None, idField='formulaId', commit=True, returnStat=False)  
        self.cntlr.addToLog(_('Formula id "{}" {}').format(formulaId, action + 'ed' if action=='insert' else action + 'd'), messageCode="RssDB.Info", file=self.conParams.get('database',''), level=logging.INFO)
        
        res = dict()
        if returnData:
            res = formulaData            
        return res
 
    def removeFormulaFromDb(self, formulaIds):
        try:
            self.dbConn.formulae.delete_many({'formulaId':{"$in": formulaIds}})
            self.addToLog(_('Removed formula(e) with id(s) {}').format(str(formulaIds)), messageCode="RssDB.Info", file=self.conParams.get('database', ''), level=logging.INFO)
        except Exception as e:
            self.addToLog(_('Error while removing formula(e) with id(s) {}:\n{}').format(str(formulaIds), str(e)), messageCode="RssDB.Error", file=self.conParams.get('database', ''), level=logging.ERROR)
        return

    def startDBReport(self, host='0.0.0.0', port=None, debug=False, asDaemon=True, fromDate=None, toDate=None, threaded=True):
        return _startDBReport(self, host, port, debug, asDaemon, fromDate, toDate, threaded=threaded)

    def close(self):
        if self.mongoClient:
            self.mongoClient.close()
            del self.mongoClient
            del self.dbConn
        return

    def checkConnection(self):
        chk = False
        try:
            chk = self.dbConn.command('dbstats')['db'] == self.dbName
        except:
            pass
        return chk

    def getReportData(self, fromDate=None, toDate=None):
        if not self.verifyCollections():
            return

        for k,v in {'From': fromDate, 'To': toDate}.items():
            if v:
                try:
                    datetime.strptime(v, '%Y-%m-%d')
                except:
                    self.cntlr.addToLog(_('{} Date is not in the correct fromat, date should be in the format yyyy-mm-dd').format(k), 
                                            messageCode="RssDB.Error", file=getattr(self, 'dbName', ''), level=logging.ERROR)
                    return
        
        if (fromDate and toDate) and (datetime.strptime(toDate, '%Y-%m-%d') <= datetime.strptime(fromDate, '%Y-%m-%d')):
            self.cntlr.addToLog(_('To Date must be later than From date'),
                                    messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
            return
        dbStats = self.getDbStats()['dictResult']
        if not fromDate and not toDate:
            lastFiling = dbStats.get('LatestFiling', None)
            if lastFiling:
                lastFilingYear = parser.parse(lastFiling).date().year
                fromDate = str(date(lastFilingYear-2, 1, 1))

        if fromDate:
            qFromDate = parser.parse(fromDate).date() if isinstance(fromDate, str) else fromDate
        
        if toDate:
            qToDate = parser.parse(toDate).date() if isinstance(toDate, str) else toDate


        q = [
            {
                '$match': {
                    'duplicate': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'cikNumber': '$cikNumber', 
                        'feedId': '$feedId', 
                        'formType': '$formType', 
                        'assignedSic': '$assignedSic', 
                        'inlineXBRL': '$inlineXBRL'
                    }, 
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'cikNumber': '$_id.cikNumber', 
                    'feedId': '$_id.feedId', 
                    'formType': '$_id.formType', 
                    'assignedSic': '$_id.assignedSic', 
                    'inlineXBRL': '$_id.inlineXBRL', 
                    'count': '$count'
                }
            }
        ]

        filingDate = {'filingDate':{}}

        if fromDate:
            filingDate['filingDate']['$gte'] = parser.parse(fromDate)
        if toDate:
            filingDate['filingDate']['$lte'] = parser.parse(toDate)

        if filingDate['filingDate']:
            q[0]['$match']['filingDate'] = filingDate['filingDate']

        filingsDataDict = list(self.dbConn.filingsInfo.aggregate(q))
        locationDict = list(self.dbConn.filersInfo.find({}, {'cikNumber':1,'conformedName':1, 'businessState':1,  '_id':0}))
        filers_lookup = {x['cikNumber']:x for x in locationDict}

        for f in filingsDataDict:
            f['conformedName'] = filers_lookup[f['cikNumber']]['conformedName']
            y = int(str(f['feedId'])[:4])
            m = int(str(f['feedId'])[-2:])
            f['feedMonth'] = str(datetime(y,m, calendar.monthrange(y,m)[1]))

        locs = list(self.dbConn.locations.find({}, {'_id':0}))
        locs_lookup = {x['code']:x for x in locs}

        for xf in locationDict:
            locator = locs_lookup.get(xf['businessState'], locs_lookup['XX'])
            xf['code'] = locator['code']
            xf['latitude'] = locator['latitude']
            xf['longitude'] = locator['longitude']
            xf['country'] = locator['country']
            xf['stateProvince'] = locator['stateProvince']
            del xf['businessState']

        with open(os.path.join(pathToSQL,'mongodbIndustryClassification.json'), 'r') as industries:
            industry = json.load(industries)

        res_industry = dict()
        for a in industry['industry']:
            if a['industry_classification'] == 'SEC':
                res_industry[str(a['industry_code'])] = {'division_name': a['ancestors'][0]['industry_description'] if  a['ancestors'] else 0}

        return dbStats, filingsDataDict, res_industry, locationDict
    
    def getDbStats(self):
        result = {'textResult': OrderedDict(), 'dictResult':OrderedDict()}
        pipe = [
            {
                '$group': {
                    '_id': 'null',
                    'LatestFiling': {
                        '$max': '$pubDate'
                    },
                    'EarliestFiling': {
                        '$min': '$pubDate'
                    },
                    'LatestFeed': {
                        '$max': '$feedId'
                    },
                    'EarliestFeed': {
                        '$min': '$feedId'
                    }
                }
            }, {
                '$project': {
                    '_id': 0
                }
            }
        ]
        if self.checkConnection():
            if self.verifyCollections(createCollections=False):
                stats = list(self.dbConn.filingsInfo.aggregate(pipe))
                lastUpdated = str(list(self.dbConn.lastUpdate.find({'id':0}, {'_id':0, 'id':0}))[0]['lastUpdate'])
                if len(stats):
                    _result = stats[0]
                    _result['CountFilers'] = self.dbConn.filersInfo.count()
                    _result['CountFilings'] = self.dbConn.filingsInfo.count()
                    _result['CountFeeds'] = self.dbConn.feedsInfo.count()
                    _result['CountFiles'] = self.dbConn.filesInfo.count()
                    _result['LastUpdate'] = lastUpdated
                    dbSize = ''
                    try:
                        dbSize = convert_size(self.dbConn.command('dbstats')['storageSize'], 'GB')[2]
                    except:
                        pass
                    _result['DatabaseSize'] = dbSize
                    result['dictResult'] = _result
                    timeSinceLastUpdate = 'Never Updated'
                    if parser.parse(_result['LastUpdate']).year == 1970:
                        _result['LastUpdate'] = None
                        timeSinceLastUpdate = 'Never Updated'
                    else:
                        td  = parser.parse(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))  - parser.parse(_result['LastUpdate'])
                        days = td.days
                        hours, remainder = divmod(td.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        timeSinceLastUpdate = '{} days, {} hours, {} minutes since last update'.format(days, hours, minutes)
                    result['textResult'] = OrderedDict([
                        ('LastUpdate', str(_result['LastUpdate']) + ' - ('+timeSinceLastUpdate+')' if _result['LatestFiling'] else 'No Data'),
                        ('CountFeeds', str(_result['CountFeeds'])),
                        ('LatestFeed', str(_result['LatestFeed'])[:4] + '-' + str(_result['LatestFeed'])[-2:] if _result['LatestFeed'] else 'No Data'),
                        ('EarliestFeed', str(_result['EarliestFeed'])[:4] + '-' + str(_result['EarliestFeed'])[-2:] if _result['EarliestFeed'] else 'No Data'),
                        ('CountFilings', str(_result['CountFilings'])),
                        ('CountFiles', str(_result['CountFiles'])),
                        ('LatestFiling', str(_result['LatestFiling']) if _result['LatestFiling'] else 'No Data'),
                        ('EarliestFiling', str(_result['EarliestFiling']) if _result['EarliestFiling'] else 'No Data'),
                        ('CountFilers', str(_result['CountFilers'])),
                        ('DatabaseSize', _result['DatabaseSize'])
                    ])
                else:
                    result['textResult'] = OrderedDict([
                        ('LastUpdate', lastUpdated),
                        ('CountFeeds', str(self.dbConn.feedsInfo.count())),
                        ('LatestFeed', 'No Data'),
                        ('EarliestFeed', 'No Data'),
                        ('CountFilings', str(self.dbConn.filingsInfo.count())),
                        ('CountFiles', str(self.dbConn.filesInfo.count())),
                        ('LatestFiling', 'No Data'),
                        ('EarliestFiling','No Data'),
                        ('CountFilers', str(self.dbConn.filersInfo.count()))
                    ])        
            else:
                result['textResult'] = {'missingCollections': ', '.join(set(rssTables) - set(self.dbConn.list_collection_names()))}
        else:
            result['textResult'] = {'noConnection': 'Could not connect to database'.format(self.conParams['database'])}

        return result
  
    def showStatus(self, msg, clearAfter=2000, end='\n'):
        if self.cntlr is not None:
            if 'end' in self.cntlr.showStatus.__code__.co_varnames:
                self.cntlr.showStatus(msg, clearAfter, end=end)
            elif isinstance(self.cntlr, CntlrCmdLine):
                print(msg, end=end)
            else:
                self.cntlr.showStatus(msg, clearAfter)
        return

    def addToLog(self, msg, **kwargs):
        if self.cntlr is not None:
            self.cntlr.addToLog(msg, **kwargs)
        return

    def changeDb(self, dbName):
        self.dbName = dbName
        self.dbConn = self.mongoClient[dbName]
        return

    def collectionsInDb(self):
        return self.dbConn.list_collection_names()

    def verifyCollections(self, createCollections=False, dropPriorCollections=False, populateFilersInfo=False):
        result = False
        missingColletions = set(rssTables) - set(self.dbConn.list_collection_names())
        # if no tables, initialize database
        if not missingColletions:
            result = True
        elif missingColletions and createCollections:
            self.create(dropPriorCollections=dropPriorCollections, populateFilersInfo=populateFilersInfo)
            if not set(rssTables) - set(self.dbConn.list_collection_names()):
                result = True
        elif missingColletions and not createCollections:
           self.addToLog(_("The following colletions are missing from {} database: {}").format(self.dbName,
               ', '.join(t for t in sorted(missingColletions))), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
        return result

    def create(self, jsonFiles=None, dropPriorCollections=False, populateFilersInfo=True):
        with open(mongodbSchemaFile, 'r') as jf:
            schemas = json.load(jf)
        opts = {'validationLevel': 'strict', 'validationAction':'error'}
        
        try:
            for c in rssTables:
                if dropPriorCollections:
                    self.dbConn.drop_collection(c)
                    self.addToLog(_('Dropped collection {}').format(c), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
                self.dbConn.create_collection(c, validator=schemas.get(c, {}), **opts)
                # There are some duplicate ticker symbols with different ciks
                if c == 'formulaeResults':
                    self.dbConn[c].create_index([(rssCols['formulaeResults'][0], DESCENDING), (rssCols['formulaeResults'][1], DESCENDING)], unique=True, background=False)
                else:
                    self.dbConn[c].create_index(rssCols[c][0], unique=False if c == 'cikTickerMapping' else True, background=False)
                self.addToLog(_('Created collection {}').format(c), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
            with open(mongodbIndustryClassificationFile, 'r') as jf:
                _data = json.load(jf)
            if _data:
                self.insertUpdateRssDB(_data['industry'], 'industry')
                self.dbConn.industry.create_index('industry_id', unique=True, background=False)
                self.insertUpdateRssDB(_data['locations'], 'locations')
                self.dbConn.locations.create_index('code', unique=True, background=False)
            updateCikTickerMapping(self)
            if populateFilersInfo:
                _populateFilersInfo(self)
            self.dbConn.lastUpdate.insert_one({'id':0, 'lastUpdate':datetime(1970,1,1,0,0,0)})
            self.dbConn.command({"create": "v_duplicate_filings",
                                 'viewOn': 'filingsInfo', 
                                 'pipeline': [{'$match': {'duplicate': 0}}, 
                                              {'$group': {'_id': '$accessionNumber', 'count': {'$sum': 1}, 'filingId': {'$min': '$filingId'}}}, 
                                              {'$match': {'count': {'$gt': 1}}}, {'$project': {'_id': 0, 'filingId': 1}}]})
            self.dbConn.command({"create": "v_filingsSummary",
                                 'viewOn': 'filingsInfo',
                                 'pipeline': [{'$match': {'duplicate': 0}}, 
                                              {'$group': {
                                                '_id': {
                                                    'cikNumber': '$cikNumber',
                                                    'feedId': '$feedId', 
                                                    'formType': '$formType', 
                                                    'assignedSic': '$assignedSic', 
                                                    'inlineXBRL': '$inlineXBRL'}, 
                                                'count': {'$sum': 1}}},
                                                {'$project': {
                                                    '_id': 0, 
                                                    'cikNumber': '$_id.cikNumber',
                                                    'feedId': '$_id.feedId', 
                                                    'formType': '$_id.formType', 
                                                    'assignedSic': '$_id.assignedSic', 
                                                    'inlineXBRL': '$_id.inlineXBRL', 
                                                    'count': '$count'}}
                                            ]})
        except Exception as e:
            self.addToLog(e._message, messageCode="RssDB.Error", file=getattr(self, 'dbName', ''),  level=logging.ERROR)
            raise e
        return

    def insertUpdateRssDB(self, inputData, dbCollection, action='insert', updateFields=None, idField=None, commit=False, returnStat=False):
        '''action either `insert` or `update` '''
        idCol = None
        if dbCollection in rssTables:
            idCol =  chkToList(idField if action=='update' and idField else rssCols[dbCollection][0], str)
        actionFunc = {"insert": self.dbConn[dbCollection].insert_many,
                    "update": self.dbConn[dbCollection].update_many}[action]
        msg = {'update':_("Updating {}"), 'insert': _('Inserting into {}')}[action].format(dbCollection)
        startInsertTime = time.perf_counter()
        _inputData = inputData if isinstance(inputData, list) else [inputData]
        res = None
        _count = 0
        if len(_inputData) > 0:
            self.showStatus(msg)
            if action == 'insert':
                from pymongo.errors import BulkWriteError
                try:
                    res = self.dbConn[dbCollection].insert_many(_inputData)
                except BulkWriteError as bwe:
                    self.showStatus(bwe.details)
                    # Can also take this component and do more analysis
                    # errors = bwe.details['writeErrors']
                    raise 
            elif action == 'update':
                from pymongo import UpdateOne
                blkIds = [UpdateOne({y: x[y] for y in idCol}, 
                                    {'$set': {y:x[y] for y in [chkToList(updateFields, str) if updateFields else x.keys()][0]}}) for x in _inputData]
                res = self.dbConn[dbCollection].bulk_write(blkIds)
        if res:
            _count = {'insert': lambda x: len(x.inserted_ids), 'update': lambda x: x.modified_count}[action](res)
        actionMsg = _('{} {} documents in {}').format(action + ('ed' if action=='insert' else 'd',)[0] , _count, dbCollection)
        self.showStatus(actionMsg)
        self.addToLog(_("Finished {} in {} secs").format(msg,
                        round(time.perf_counter() - startInsertTime, 3)),
                        messageCode="RssDB.Info", file=self.conParams.get('database', ''),  level=logging.INFO)
        
        result = None
        if returnStat:
            result = {action: _count}
        return result

    def xdoAll(self, loc=None, last=None, dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, 
                maxWorkers=None, updateDB=True, reloadCache=False, updateExisting=True, refreshAll=False, 
                timeOut=3, retries=3, includeLatest=False, getFiles=True, getXML=False, getFilers=True, updateTickers=False, q=None):
        '''Creates and populates rssDB'''
        return _doAll(self, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, getRssItems=getRssItems, returnInfo=returnInfo,
                     maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, refreshAll=refreshAll,
                     timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, 
                     getXML=getXML, getFilers=getFilers, updateTickers=updateTickers, q=q)

    
    def doAll(self, setAutoUpdate=False, waitFor=timedelta(minutes=wait_duration), duration=timedelta(hours=1), loc=None, last=None, 
                dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, maxWorkers=None, updateDB=True, reloadCache=False, 
                updateExisting=True, refreshAll=False, timeOut=3, retries=3, includeLatest=True, getFiles=True, getXML=False, 
                getFilers=True, updateTickers=False, q=None):
        '''Creates and populates rssDB'''
        return _doAll(self, setAutoUpdate=setAutoUpdate, waitFor=waitFor, duration=duration, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, 
                            getRssItems=getRssItems, returnInfo=returnInfo, maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, 
                            refreshAll=refreshAll, timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML,
                            getFilers=getFilers, updateTickers=updateTickers, q=q)


    def updateRssFeeds(self, loc=None, getRssItems=False, updateDB=False, maxWorkers=None, returnInfo=False,
                        dateFrom=None, dateTo=None, last=None, reloadCache=False, includeLatest=False, getFiles=True, getXML=False, q=None):
        return _updateRssFeeds(self, loc=loc, getRssItems=getRssItems, updateDB=updateDB, maxWorkers=maxWorkers, returnInfo=returnInfo,
                        dateFrom=dateFrom, dateTo=dateTo, last=last, reloadCache=reloadCache, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML, q=q)


    def updateFilersInfo(self, updateExisting=False, refreshAll=False, updateDB=False, 
                            maxWorkers=None, timeOut=3, retries=3, returnData=False):
        hasTables = rssTables[3] in self.dbConn.list_collection_names()
        startTime = time.perf_counter()
        newCiksData = []
        updatedExistingCiksData = []
        if refreshAll and hasTables:
            self.dbConn[rssTables[3]].remove({})
        allNewCikData = 0
        if hasTables:
            distinctCiks_filings = self.dbConn[rssTables[1]].distinct('cikNumber')
            distinctCiks_filers = self.dbConn[rssTables[3]].distinct('cikNumber')
            _newCiksList = list(set(distinctCiks_filings) - set(distinctCiks_filers))
            _newCiks_patches = [_newCiksList[i:i + 100] for i in range(0, len(_newCiksList), 100)]
            indx = 1
            for nP, patch in enumerate(_newCiks_patches):
                self.showStatus(_('Retriving new CIKs patch {} of {}').format(nP + 1, len(_newCiks_patches)))
                _newCiksData = getFilerInformation(self, patch, timeOut=timeOut, maxWorkers=maxWorkers, i=indx, _all=len(_newCiksList), retries=retries)
                newCiksData = [x['filerInfo'] for x in _newCiksData['retrived']]
                indx = _newCiksData.get('i')
                if updateDB:
                    if len(newCiksData)>0:
                        self.showStatus(_('Inserting new ciks patch {} of {}').format(nP + 1, len(_newCiks_patches)))
                        self.insertUpdateRssDB(newCiksData, rssTables[3], action='insert')
                        allNewCikData += len(newCiksData)
        allUpdatedExistingCiksData = 0
        if updateExisting and hasTables:
            self.showStatus(_('Retriving ciks with changes'))
            # check for existing filers where name in recent filings is different from filers information
            # when pubDate of the filing is later than last name change then propably filer information 
            # needs to be refreshed, not the most accurate test but can hint changes in filer's info

            # Get most recent filing by cik
            _recentFilings = self.dbConn.filingsInfo.aggregate(
                [
                    {"$sort":{"filingId": -1}},
                    {"$group": {"_id": "$cikNumber", "lastDoc": {"$first": "$$ROOT"}}},
                    {"$project": {"_id":0, "cikNumber": "$lastDoc.cikNumber", "companyName": "$lastDoc.companyName", "pubDate": "$lastDoc.pubDate"}}
                ], allowDiskUse=True
            )
            recentFilings = list(_recentFilings)

            # Get filer information
            filers = list(self.dbConn.filersInfo.find({}, {"_id":0, "conformedName":1, "cikNumber":1, "formerNames":1 }))
            # sorting makes the below lookup more efficient 
            xRecentFilings = sorted(recentFilings, key=lambda x: x['cikNumber'])
            xFilers = sorted(filers, key=lambda x: x['cikNumber'])

            # Return filers with name in filing different from name in filer's info (indicates name change)
            chk_dict = []
            for x in xRecentFilings:
                y = None
                for n, flr in enumerate(xFilers):
                    if flr['cikNumber'] == x['cikNumber']:
                        y = xFilers.pop(n)
                        break
                if y:
                    if not y['conformedName'].lower() == x['companyName'].lower():
                        chk_dict.append({**x, **y})

            to_refresh = []
            for d in chk_dict:
                # make sure former names are parsed, and in revese order (latest name change in [0] position)
                formerNames = d['formerNames'] if isinstance(d['formerNames'], list) else json.loads(d['formerNames'] if d['formerNames'] else '[]')
                formerNames.sort(key=lambda x: parser.parse(x['date']) if isinstance(x['date'], str) else x['date'], reverse=True)
                # no former names but still conformed name != company name, then refresh information
                formerNameDate = date.min
                if len(formerNames) > 0:
                    formerNameDate = parser.parse(formerNames[0]['date']).date() if isinstance(formerNames[0]['date'], str) else formerNames[0]['date'].date()
                pubDate = parser.parse(d['pubDate']).date() if isinstance(d['pubDate'], str) else d['pubDate'].date()
                if pubDate > formerNameDate:
                    to_refresh.append(d['cikNumber'])
            if len(to_refresh) > 0:
                to_refresh_patches = [to_refresh[i:i + 100] for i in range(0, len(to_refresh), 100)]
                indx2 = 1
                for nP2, patch2 in enumerate(to_refresh_patches):
                    self.showStatus(_('Retriving CIKs with changes patch {} of {}').format(nP2, len(to_refresh_patches)))
                    _updatedExistingCiksData = getFilerInformation(self, patch2, timeOut=timeOut, maxWorkers=maxWorkers, i=indx2, _all=len(to_refresh), retries=retries)
                    updatedExistingCiksData = [x['filerInfo'] for x in _updatedExistingCiksData['retrived']]
                    indx2 = _updatedExistingCiksData.get('i')
                if updateDB:
                    if len(updatedExistingCiksData) > 0:
                        self.showStatus(_('Updating ciks patch {} of {}').format(nP2, len(to_refresh_patches)))
                        self.insertUpdateRssDB(updatedExistingCiksData, rssTables[3], action='update')
                        allUpdatedExistingCiksData += len(updatedExistingCiksData)       
        endTime = time.perf_counter()
        _msg = _('Finished updating filers information in {} secs').format(round(endTime-startTime,3))
        self.showStatus(_msg)
        result = {'summary':{rssTables[3]:{'insert': allNewCikData, 'update': allUpdatedExistingCiksData}}, 'stats': _msg}
        if returnData:
            result['newCiks'] = newCiksData
            result['updatedCiks'] = updatedExistingCiksData  

        return result


    def getMonthlyFeedsLinks(self, loc=None, maxWorkers=None, dateFrom=None, dateTo=None, last=None):
        """Returns Monthly feeds Links  that are not in DB or with lastModified date later than in DB"""
        
        feeds, compareCol, startTime = _getMonthlyFeedsLinks(self, loc=loc, maxWorkers=maxWorkers, dateFrom=dateFrom, dateTo=dateTo, last=last)

        results = []

        if feeds:
            feeds.sort(key=lambda x: x['feedId'])
            if any((dateFrom, dateTo)):
                _from = parser.parse(dateFrom) if dateFrom else datetime.min
                _to = parser.parse(dateTo) if dateTo else datetime.max
                if _to <= _from:
                    raise Exception('dateTo must be later than dateFrom')
                _feeds = [x for x in feeds if x['feedDate'] >= _from and x['feedDate'] <= _to]
                feeds = _feeds
            if last:
                _feeds = feeds[-last:]
                feeds = _feeds
            existing = []
            existingIds = []
            if all([x in self.dbConn.list_collection_names() for x in rssTables[:2]]) :
                self.showStatus(_('Getting existing feeds information'))
                _existing = self.dbConn[rssTables[0]].find({}, {rssCols[rssTables[0]][0]: 1, compareCol: 1, "_id": 0}) 
                existing = [(x['feedId'], parser.parse(x[compareCol], tzinfos={'EST':'UTC-5:00', 'EDT':'UTC-4:00'}) if isinstance(x[compareCol], str) else x[compareCol])
                            for x in _existing]
                existingIds = [i[0] for i in existing]
            else:
                self.showStatus(_('Rss DB collections not intialized, returning all available feeds'))
            for x in feeds:
                if x['feedId'] not in existingIds:
                    x['isNew'] = True
                    results.append(x)
                    self.addToLog(_('New: {}').format(x['link']), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
                # first check modified date to avoid downloading and parsing feeds with no new entries
                elif existing[existingIds.index(x['feedId'])][1]:
                    if x[compareCol] > existing[existingIds.index(x['feedId'])][1]:
                        x['isNew'] = False
                        results.append(x)
                        self.addToLog(_('Updatable: {}').format(x['link']), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
                # finally just download and parse the document and compare it to existing
                else:
                    x['isNew'] = False
                    results.append(x)
                    self.addToLog(_('May need update: {}').format(x['link']), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
                # leaves out existing ids with compareCol date not later than new lastModifiedDate\lastBuildDate
        endTime = time.perf_counter()
        self.addToLog(_('Finished getting SEC monthly XBRL RSS Feed links in {} sec(s)').format(round(endTime-startTime, 3)), messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
        return results


    def getFeedInfo(self, link, lastModifiedDate, isNew, reloadCache=False, getFiles=True, getXML=False):
        """Gets feed info ready to insert in db"""
        startAllTime = time.perf_counter()
        feedInfo, _rssItemsList = _getFeedInfo(self, link, lastModifiedDate, isNew, reloadCache)
        f_id = int(str(feedInfo['feedId']) + '100000' ) + 1
        if not isNew  and rssTables[1] in self.dbConn.list_collection_names():
            _max_filings_id = list(self.dbConn[rssTables[1]].aggregate([
                                    {"$match": {"feedId": feedInfo['feedId']}},
                                    {"$group": {"_id": "$feedId","maxId": {"$max": "$filingId"}}},
                                    {"$project": {"_id": 0, "maxId": 1}}
                                    ]))

            max_filings_id = _max_filings_id[0]['maxId'] if _max_filings_id else None
            
            if max_filings_id:
                f_id = max_filings_id + 1
        result = { x:[] for x in rssTables}
        for rssI in _rssItemsList:
            itemInfo = getRssItemInfo(rssI, feedInfo['feedId'], f_id, getFiles, getXML)
            for _k in itemInfo.keys():
                result[_k].append(itemInfo[_k]) 
            f_id +=1
        result[rssTables[0]] = feedInfo
        result['isNew'] = isNew
        result['logMsg'] = _("Finished extracting data from {} in {} secs").format(link, round(time.perf_counter() - startAllTime, 3))
        try:
            logs = self.cntlr.logHandler.getLines()
        except:
            pass
        self.cntlr.modelManager.close()
        gc.collect()
        # self.addToLog(result['logMsg'], messageCode="RssDB.Info", file="",  level=logging.INFO)
        return result


    def getExistingFeeds(self):
        feedsIds = []
        if rssTables[0] in self.dbConn.list_collection_names():
            feedsIds = [x[rssCols[rssTables[0]][0]] for x in self.dbConn[rssTables[0]].find({}, {rssCols[rssTables[0]][0]:1, "_id":0})]

        return feedsIds


    def updateDuplicateFilings(self):
        statTime = time.perf_counter()
        dups = [x['filingId'] for x in self.dbConn.v_duplicate_filings.find({})]
        _count_files = 0
        _count_filings = 0
        if len(dups):
            res_filings = self.dbConn[rssTables[1]].update_many({'filingId': {'$in': dups}}, {'$set': {'duplicate': 1}})
            _count_filings = res_filings.modified_count
            self.addToLog(_('updated {} documents in {}').format( _count_filings, 'filingsInfo'),
                                messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
            res_files = self.dbConn[rssTables[2]].update_many({'filingId': {'$in': dups}}, {'$set': {'duplicate': 1}})
            _count_files = res_files.modified_count
            self.addToLog(_('updated {} documents in {}').format( _count_files, 'filesInfo'), 
                                messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
        else:
            self.showStatus('No duplicates to update')
        stat = {'filingsInfo': {'update': _count_filings}, 'filesInfo': {'update':_count_files}}   
        endTime = round(time.perf_counter() - statTime, 3)
        msg = _('Finished updating duplicate filings in {} sec(s)').format(endTime)
        self.addToLog(msg, messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)
        stat['msg'] = msg
        return stat
           

    def getById(self, idsList, collectionName, idField=None, idDataType=int, returnFields=None, additionalWhereClauseDict=None):
        '''Get rows by ids from specified tables with optional where clause'''
        result = None
        if not idsList:
            raise Exception('Now ids to get')

        _idsList = chkToList(idsList, idDataType)
        _returnFields = dict()
        if returnFields:
            returnFields = chkToList(returnFields, str)
            _returnFields = {x:1 for x in returnFields}
        
        if additionalWhereClauseDict:
            if not isinstance(additionalWhereClauseDict, dict):
                raise Exception('additionalWhereClauseDict must be a dict')
        elif not additionalWhereClauseDict:
            additionalWhereClauseDict = dict()
        
        if not idField:
            idField = rssCols[collectionName][0]
        idsFilter = {idField : { "$in": _idsList}}

        result = list(self.dbConn[collectionName].find({**idsFilter, **additionalWhereClauseDict}, {"_id":0, **_returnFields}))
        return result


    def searchFilings(self, companyName=None, tickerSymbol=None, cikNumber=None, formType=None, assignedSic=None, 
                        dateFrom=None, dateTo=None, inlineXBRL=None, limit=100, getFiles=False, filingIds=None, **kwargs):
        # accommodate both list and string input
        resultDict = dict(filings=[], files=[])
        filingsDicts = {}
        if not filingIds: # shortcut
            companyName = ','.join(companyName) if isinstance(companyName, (list, tuple, set)) else companyName
            tickerSymbol = ','.join(tickerSymbol) if isinstance(tickerSymbol, (list, tuple, set)) else tickerSymbol
            cikNumber = ','.join(cikNumber) if isinstance(cikNumber, (list, tuple, set)) else cikNumber
            formType = ','.join(formType) if isinstance(formType, (list, tuple, set)) else formType
            assignedSic = ','.join([str(x) for x in assignedSic]) if isinstance(assignedSic, (list, tuple, set)) else assignedSic

            inlineFilter = {
                'yes': 1,
                'no': 0
            }
            if not limit:
                limit = 100
            whereClause = OrderedDict([
                ('companyName', ['%' + x.strip() + '%' for x in companyName.split(',')] if companyName else []),
                ('tickerSymbol', [x.strip() for x in tickerSymbol.split(',')] if tickerSymbol else []),
                ('cikNumber', [x.strip() for x in cikNumber.split(',')] if cikNumber else []),
                ('formType', ['%' + x.strip() + '%' for x in formType.split(',')] if formType else []),  
                ('assignedSic', [int(x.strip()) for x in assignedSic.split(',')] if assignedSic else []), 
                ('dateFrom', [dateFrom] if dateFrom else []),
                ('dateTo', [dateTo] if dateTo else []),
                ('inlineXBRL', [str(inlineFilter[inlineXBRL.lower()])] if inlineXBRL else []),
                ('limit', [limit] if limit else [100])])
            
            self.showStatus(_('Retriving Data'))
        
            t = list(self.dbConn.cikTickerMapping.find({'tickerSymbol': {'$in': whereClause['tickerSymbol']}},
                                                    {"cikNumber":1, 'tickerSymbol':1, "_id":0})) if whereClause['tickerSymbol'] else None
            res_t = [x['cikNumber'] for x in t] if t else []
            res_t_dict = {x['cikNumber']: x['tickerSymbol'] for x in t} if t else {}

            mongoQry = dict()
            if any([whereClause['companyName'], whereClause['cikNumber'], res_t]):
                mongoQry['$or'] = []
                if whereClause['companyName']:
                    mongoQry['$or'].append({"companyName": {"$regex": '^.*({}).*$'.format('|'.join(whereClause['companyName']).replace('%', '')), '$options': 'i'}})
                if whereClause['cikNumber'] or res_t:
                    allCik = [*whereClause['cikNumber'], *res_t] if res_t else whereClause['cikNumber'][:]
                    if allCik:
                        mongoQry['$or'].append({'cikNumber': {'$in': allCik}})
            if whereClause['formType']:
                mongoQry['formType'] = {"$regex":'^.*({}).*$'.format('|'.join(whereClause['formType']).replace('%', '')),'$options': 'i'}
            if whereClause['assignedSic']:
                mongoQry['assignedSic'] = {'$in': whereClause['assignedSic']}
            if dateFrom or dateTo:
                mongoQry['filingDate'] = {}
                if dateFrom:
                    mongoQry['filingDate']['$gte'] = datetime.strptime(dateFrom, '%Y-%m-%d')
                if dateTo:
                    mongoQry['filingDate']['$lte'] = datetime.strptime(dateTo, '%Y-%m-%d')
            if inlineXBRL:
                mongoQry['inlineXBRL'] = inlineFilter[inlineXBRL.lower()]

            mongoQry_result = self.dbConn.filingsInfo.find(mongoQry, {'_id':0}, sort=[( 'filingId',  DESCENDING )]).limit(limit)
            filingsDicts = list(mongoQry_result)
            if res_t_dict:
                for d in filingsDicts:
                    d['tickerSymbol'] = res_t_dict.get(d['cikNumber'], None)
        else:
            filingIds = filingIds.split(',') if isinstance(filingIds, str) else filingIds
            mongoQry_result = self.dbConn.filingsInfo.find({'filingId': {'$in':filingIds}}, {'_id':0})
            filingsDicts = list(mongoQry_result)

        resultDict['filings'] = filingsDicts
        if getFiles and filingsDicts:
            filings_ids = [x['filingId'] for x in filingsDicts]
            resultDict['files'] = list(self.dbConn.filesInfo.find({'filingId': {'$in':[x['filingId'] for x in filingsDicts]}}, {'_id':0}))

        self.addToLog(_('Retrived {} filing(s) and {} file(s)').format(len(resultDict['filings']), len(resultDict['files'])),
                            messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)

        return resultDict


    def searchFilers(self, companyName=None, tickerSymbol=None, cikNumber=None, industry=None, limit=100, **kwargs):
        # accommodate both list and string input
        companyName = ','.join(companyName) if isinstance(companyName, (list, tuple, set)) else companyName
        tickerSymbol = ','.join(tickerSymbol) if isinstance(tickerSymbol, (list, tuple, set)) else tickerSymbol
        cikNumber = ','.join(cikNumber) if isinstance(cikNumber, (list, tuple, set)) else cikNumber
        industry = ','.join([str(x) for x in industry]) if isinstance(industry, (list, tuple, set)) else industry
        if not limit:
            limit = 100
        whereClause = OrderedDict([
            ('conformedName', ['%' + x.strip() + '%' for x in companyName.split(',')] if companyName else []),
            ('tickerSymbol', [x.strip() for x in tickerSymbol.split(',')] if tickerSymbol else []),
            ('cikNumber', [x.strip() for x in cikNumber.split(',')] if cikNumber else []),
            ('industry', [int(x.strip()) for x in industry.split(',')] if industry else []), 
            ('limit', [limit] if limit else [100])])
        
        self.showStatus(_('Retriving Data'))
    
        t = list(self.dbConn.cikTickerMapping.find({'tickerSymbol': {'$in': whereClause['tickerSymbol']}},
                                                {"cikNumber":1, 'tickerSymbol':1, "_id":0})) #if whereClause['tickerSymbol'] else None
        res_t = [x['cikNumber'] for x in t] if t else []
        res_t_dict = {x['cikNumber']: x['tickerSymbol'] for x in t} if t else {}

        mongoQry = dict()
        if any([whereClause['conformedName'], whereClause['cikNumber'], res_t]):
            mongoQry['$or'] = []
            if whereClause['conformedName']:
                mongoQry['$or'].append({"conformedName": {"$regex": '^.*({}).*$'.format('|'.join(whereClause['conformedName']).replace('%', '')), '$options': 'i'}})
            if whereClause['cikNumber'] or res_t:
                allCik = [*whereClause['cikNumber'], *res_t] if res_t else whereClause['cikNumber'][:]
                if allCik:
                    mongoQry['$or'].append({'cikNumber': {'$in': allCik}})
        if whereClause['industry']:
            mongoQry['industry_code'] = {'$in': whereClause['industry']}
        mongoQry_result = self.dbConn.filersInfo.find(mongoQry, {'_id':0}).limit(limit)
        resultDict = dict(filers=[])
        filersDicts = list(mongoQry_result)

        t2 = list(self.dbConn.cikTickerMapping.find({'cikNumber': {'$in': [x['cikNumber'] for x in filersDicts]}},
                                                {"cikNumber":1, 'tickerSymbol':1, "_id":0})) #if whereClause['tickerSymbol'] else None
        t2_dict = {x['cikNumber']: x['tickerSymbol'] for x in t2 if not '-' in x['tickerSymbol']}
        for d in filersDicts:
            d['tickerSymbol'] = t2_dict.get(d['cikNumber'], None)
        
        # make tickers unique
        unique_ciks = {x['cikNumber'] for x in filersDicts}
        unique_filers_dicts = []
        for cik in unique_ciks:
            tickersSet = set()
            # find all tickers
            for t in filersDicts:
                if t['cikNumber'] == cik:
                    if not t['tickerSymbol'] is None:
                        tickersSet.add(t['tickerSymbol'])
            # get 1 filer info
            for d in filersDicts:
                if d['cikNumber'] == cik:
                    d['tickerSymbol'] = '|'.join(tickersSet)
                    unique_filers_dicts.append(d)
                    break
        resultDict['filers'] = unique_filers_dicts

        self.addToLog(_('Retrived {} filer(s) with {} ticker symbol(s)').format(len(unique_filers_dicts), len(filersDicts)),
                            messageCode="RssDB.Info", file=getattr(self, 'dbName', ''),  level=logging.INFO)

        return resultDict

    def dumpFilersInfo(self):
        """Creates dumps filers table to a pickle file 
        This is useful when creating db as retriving filers information consumes a lot of time because in addition
        to the time consumed in parsing each filer's info page, SEC has a 10 requests per second limit, so the process
        needs to be slowed down to accommodate the request limit.

        When creating db a choice can be made whether to use stored filers resulting from this dump or retrive from SEC site.

        `WARNING` eventhough the update process tries to detect and update outdated filers' information, filers' information
        might still be outdated, every now and then it is good to select 'refreshAll' option when updating the db to retrive ALL
        current filers' information from the SEC site to refresh filersInfo table.
        """
        filersInfo = list(self.dbConn.filersInfo.find({}, {"_id":0}))
        filersInfoDict = {'retrivedOn': datetime.now().replace(microsecond=0), 'sourceDBType':self.product, 'data': filersInfo}
        fileName = os.path.join(pathToSQL, '{}_filersInfo.pkl'.format(self.product))
        self.addToLog(_('Creating/updating filers dump'), messageCode='RssDB.Info', file=self.conParams.get('database', ''), level=logging.INFO)
        with open(fileName, 'wb') as f:
            pickle.dump(filersInfoDict, f)
        return

    def get_existing_filing_numbers(self, form_types:list=None):
        '''List of filing numbers (accession numbers) of form_types existing in db
        returns fields: 
            filingsInfo.filingId, filingsInfo.accessionNumber, filingsInfo.formType, filingsInfo.acceptanceDatetime
        '''
        existing_filings = None
        return_fields = {"filingId":1, "accessionNumber":1, "formType":1, "acceptanceDatetime":1 ,"_id":0}
        xfilter = {'duplicate':0}

        if form_types is not None:
            if isinstance(form_types, (list, tuple, set)):
                form_types = [str(x) for x in form_types]
                xfilter['formType'] = {'$in': form_types}
            else:
                form_types = str(form_types)
                xfilter['formType'] = form_types
        existing_filings = list(self.dbConn.filingsInfo.find(xfilter, return_fields))
        return existing_filings



def _makeMongoDBIndustryClassifications(cntlr, relativesFields = ['industry_id', 'industry_code', 'industry_description', 'depth'] ):
    '''Creates industry collection data based on industry table in semantic model
    
    Every industry classification document include the industry's children, siblings and ancestor
    based on a combination of the tree structure approaches in mongodb documentation.
    https://docs.mongodb.com/manual/applications/data-models-tree-structures/

    Information included for 'relatives' can be controlled using relativesFields.
    '''

    # Get industry table from semantic model
    xdbCon = rssSqlDbConnection(cntlr=cntlr, user='', password='', host='', port='', database=':memory:', timeout=20,
                                    product='sqlite', schema='')
    xdbCon.verifyTables()
    industry = xdbCon.execute('Select * from industry', fetch=True, close=False)
    colNames = [x[0] for x in xdbCon.cursor.description]
    industry_dicts = [OrderedDict(zip(colNames,x)) for x in industry]
    industry_cp = industry_dicts[0:]

    depth_q = xdbCon.execute('Select industry_classification, max(depth) as depth from industry_structure group by industry_classification', fetch=True, close=False)
    depth = {x[0]:x[1] for x in depth_q}

    _relativesFields = chkToList(relativesFields, str)

    # children -- industries with parent id == industry id
    # siblings -- industries with same parent id
    result = []
    for d in industry_cp:
        d['children'] = [OrderedDict((_k, x[_k]) for _k in _relativesFields) for x in industry_dicts if x['parent_id'] == d['industry_id']]
        d['siblings'] = [OrderedDict((_k, x[_k]) for _k in _relativesFields) for x in industry_dicts if x['parent_id'] == d['parent_id'] and not x['industry_id'] == d['industry_id']]
        result.append(d)

    # ancestors -- climbs up the tree based on parent_ids
    for res in result:
        res['ancestors'] = []
        parent = res['parent_id']
        while parent:
            parent_dict = [
                x for x in industry_dicts if x['industry_id'] == parent][0]
            res['ancestors'].insert(0, OrderedDict((_k, parent_dict[_k]) for _k in _relativesFields))
            parent = parent_dict['parent_id']

        d = depth[res['industry_classification']]
        path_list = res['ancestors'][0:]
        path_list.append(OrderedDict((_k,res[_k]) for _k in _relativesFields))
        diff = depth[res['industry_classification']] - len(path_list)
        if diff > 0:
            path_list.extend([None]*diff)
        res['path'] = path_list
    
    return result

