'''Create and update a simple database to track `Monthly` SEC rss for XBRL filings

Based on xbrlDB plugin, this plugin creates a database to track filings available through
SEC monthly rss feeds for XBRL filings, and provide tools to work with filings information
included in these feeds. Selected data points can be attached to each filing, to 
provide for quick analysis without having to store the whole filing.

`IMPORTANT`: This module works on `Monthly` RSS feeds list available at :
https://www.sec.gov/Archives/edgar/monthly/
During the month the current month file is rebuilt to include most recent data
and will usually contain filings up to date.
'''

import os, pathlib, logging, time, atexit, threading, gettext
from datetime import datetime, timedelta
from collections import defaultdict

parentDir = list(pathlib.Path(__file__).parents)[1]
pathTo_arellepy = str(os.path.join(parentDir, 'arellepy'))

if not os.path.exists(pathTo_arellepy):
    raise Exception(_('Cannot find arellepy in parent directory'))

def rssDBToolBarExtender(cntlr, toolbar):
    try:
        from .RssDBPanel import toolBarExtender
    except:
        from RssDBPanel import toolBarExtender

    toolBarExtender(cntlr=cntlr, toolbar=toolbar)
    return

def rssDBCmdLineOptionExtender(parser, *args, **kwargs):
    # DB connection group
    parser.add_option("--rssDBconnect", action='store_true', dest="rssDBconnect", default=False, help=_("Flag whether to connect to db"))
    parser.add_option("--rssDBuser", action='store', dest="rssDBuser", default='', help=_("Database username if applicable"))
    parser.add_option("--rssDBpassword", action='store', dest="rssDBpassword", default='', help=_("Database password if applicable"))
    parser.add_option("--rssDBhost", action='store', dest="rssDBhost", default='', help=_("Host name for the database, not necessary for sqlite dbs"))
    parser.add_option( "--rssDBport", action='store', dest="rssDBport", default=None, type='int', help=_("Port number on host if applicable"))
    parser.add_option( "--rssDBdatabase", action='store', dest="rssDBdatabase", default=None, help=_("Database name"))
    parser.add_option("--rssDBtimeout", action='store', dest="rssDBtimeout", default=20, type='int', help=_("Timeout for trying to connect, default 20 secs"))
    parser.add_option("--rssDBproduct", action='store', dest="rssDBproduct", type = "choice", 
                        choices = ['sqlite', 'postgres', 'mongodb'], help=_("Database type, on of 'sqlite', 'postgres', 'mongodb'"))
    parser.add_option("--rssDBschema", action='store', dest="rssDBschema", default='rssFeeds', help=_("Schema to connect to, only relevant for postgres"))
    parser.add_option( "--rssDBcreateSchema", action='store_true', dest="rssDBcreateSchema", help=_("Create Schema if doesn't exist (named as name supplied to rssDBschema option)"))
    parser.add_option("--rssDBcreateDB", action='store_true', dest="rssDBcreateDB", help=_("Create Database if doesn't exist (named as name supplied to rssDBdatabase option)"))
    

    # db create/update/doAll group
    parser.add_option("--rssDBupdate", action='store_true', dest="rssDBupdate", default=False, help=_("Optional - Flag to Update DB given rest of the rssDBupdate~ options"))
    parser.add_option("--rssDBupdateDateFrom", action='store', dest="rssDBupdateDateFrom", default=None, help=_("Optional - From Date for date range to update formated as yyy-mmm-dd"))
    parser.add_option("--rssDBupdateDateTo", action='store', dest="rssDBupdateDateTo", default=None, help=_("Optional - To Date for date range to update formated as yyy-mmm-dd"))
    parser.add_option("--rssDBupdateDoNOTGetLatest", action='store_false', dest="rssDBupdateDoNOTGetLatest", default=True, help=_("Optional - Flag to stop update from retriving latest filing not yet in the monthly archived feeds on SEC website"))
    parser.add_option("--rssDBupdateMaxWorkers", action='store', dest="rssDBupdateMaxWorkers", default=None, help=_("Optional - max number of processes to use during the update, defaults to half available cpus"))
    
    parser.add_option("--rssDBupdateEnableAuto", action='store_true', dest="rssDBupdateEnableAuto", default=False, 
                        help=_("Optional - Flag to enable auto-update, if set, runs a thread to check SEC site for updates every 10 minutes for the next period of time specified by" 
                                "--rssDBupdateEnableAutoDays, --rssDBupdateEnableAutoHours and --rssDBupdateEnableAutoMinutes"))
    
    parser.add_option("--rssDBupdateEnableAutoDays", action='store', dest="rssDBupdateEnableAutoDays", default=0, type='int',
                        help=_("Options - integer specifying number of days to keep updating the database, default 0 day(s)"))
    parser.add_option("--rssDBupdateEnableAutoHours", action='store', dest="rssDBupdateEnableAutoHours", default=1, type='int',
                        help=_("Options - integer specifying number of hours to keep updating the database, default 1 hour(s)"))
    parser.add_option("--rssDBupdateEnableAutoMinutes", action='store', dest="rssDBupdateEnableAutoMinutes", default=0, type='int',
                        help=_("Options - integer specifying number of minutes to keep updating the database, default 0 minute(s)"))

    # db report group
    parser.add_option("--rssDBreportlaunch", action='store_true', dest="rssDBreportlaunch", help=_("Flag to launch db report (dash/flask flask app)"))
    parser.add_option("--rssDBreporthost", action='store', default='0.0.0.0', dest="rssDBreporthost", help=_("Host for db report defaults to 0.0.0.0"))
    parser.add_option("--rssDBreportport", action='store', dest="rssDBreportport", help=_("Port for db report"))
    parser.add_option("--rssDBreportdebug", action='store_true', default=False, dest="rssDBreportdebug", help=_("Flag to launch flask app in debug"))
    parser.add_option("--rssDBreportfromDate", action='store', dest="rssDBreportfromDate", help=_("Initial report view date range formated as yyy-mmm-dd"))
    parser.add_option("--rssDBreporttoDate", action='store', dest="rssDBreporttoDate", help=_("Initial report view date range  formated as yyy-mmm-dd"))

    # db search filings group
    parser.add_option("--rssDBsearch", action='store_true', dest="rssDBsearch", default=False, help=_("Flag to initiate search db"))
    # Result limiters
    parser.add_option("--rssDBsearchdateFrom", action='store', dest="rssDBsearchdateFrom", 
                            help=_("Filing date in the format: yyyy-mm-dd, if left empty, gets from the earliest filing date, filings are sorted descending by filing date, LIMITS the query to this start date."))
    parser.add_option("--rssDBsearchdateTo", action='store', dest="rssDBsearchdateTo", 
                        help=_("Filing date in the format: yyyy-mm-dd, if left empty, gets up to the latest filing date, filings are sorted descending by filing date, LIMITS the query to this end data."))
    parser.add_option("--rssDBsearchassignedSic", action='store', dest="rssDBsearchassignedSic", 
                            help=_("Comma separated SEC industry code(s), LIMITS the query to selected industries"))
    parser.add_option("--rssDBsearchinlineXBRL", action='store', dest="rssDBsearchinlineXBRL", choices = ['yes', 'no'],
                            help=_("True, False or empty, True returns ONLY inlineXbrl filings, False returns ONLY non inlineXbrl, empty returns ALL"))
    parser.add_option("--rssDBsearchlimit", action='store', type='int', dest="rssDBsearchlimit", help=_("Limits the number of rows returned by query"))
    # Filers selection
    parser.add_option("--rssDBsearchcompanyName", action='store', dest="rssDBsearchcompanyName", 
                        help=_("Comma separated companies names or part thereof, example: microsoft, General Electric,... \n Looks for the EXACT OR SIMILAR name."))
    parser.add_option("--rssDBsearchtickerSymbol", action='store', dest="rssDBsearchtickerSymbol", 
                        help=_("Comma separated tickers, example: msft, gm,... \n Looks for the EXACT ticker in addition to company names in the company names field."))
    parser.add_option("--rssDBsearchcikNumber", action='store', dest="rssDBsearchcikNumber", 
                        help=_("Comma separated cik Numbers, example: 0001234567, 0007654321,... \n Looks for the EXACT CIK Number(s) in addition to ticker(s) and companies names."))
    parser.add_option("--rssDBsearchformType", action='store', dest="rssDBsearchformType",
                        help=_("Comma separated SEC form type, example: 10-K, 10-Q,..., limits the query to the selected form(s)"))
    # Result save
    parser.add_option("--rssDBsearchresultFile", action='store', dest="rssDBsearchresultFile",
                        help=_("Absolute path to file to save query result as an RSS feed (.xml) that can be processed by arelle"))
    
    # Run rssDB edgar render
    parser.add_option("--rssDBSearchrenderEdgarReports", action='store_true', default=False, dest="rssDBSearchrenderEdgarReports",
                        help=_("Render Edgar reports for the result of the db query -- folder name to store the rendered reports must be given for the param --rssDBsearchEdgarRenderFolder"))
    parser.add_option("--rssDBsearchEdgarRenderFolder", action='store', dest="rssDBsearchEdgarRenderFolder",
                    help=_("Output folder for rendered Edgar reports"))
    parser.add_option("--rssDBsearchEdgarRenderPlugins", action='store', dest="rssDBsearchEdgarRenderPlugins",
                    help=_("Comma separated values for absolute paths to required plugins 'validate/EFM', 'EdgarRenderer','transforms/SEC'."
                           "None if using default plugins location of Arelle installation."))
    parser.add_option("--rssDBsearchEdgarViewer", action='store_true', default=False, dest="rssDBsearchEdgarViewer",
                    help=_("Launches Edgar Viewer initially with the rendered reports from --rssDBSearchrenderEdgarReports"))

    parser.add_option("--rssDBFormulaRemoveDups", action='store_true', dest="rssDBFormulaRemoveDups", default=False, 
                        help=_("Flag whether to remove duplicates after formula processing uses class 'ValidateFormula.Finished'"))

    # Add formula to db
    parser.add_option("--rssDBAddFormula", action='store_true', dest="rssDBAddFormula", default=False, 
                        help=_("Flag whether to add formula to db"))
    
    parser.add_option("--rssDBAddFormulaFileName", action='store', dest="rssDBAddFormulaFileName",
                        help=_("File name for formula linkbase, either this or '--rssDBAddFormulaLinkBaseString' must be entered"))
    
    parser.add_option("--rssDBAddFormulaFormulaId", action='store', dest="rssDBAddFormulaFormulaId", type='int',
                        help=_("Optional integer, if id is for formula existing in db and flag 'rssDBAddFormulaReplaceExistingFormula' is set to true, the existing formula will be updated, otherwise the new formula added with this id"))
    
    parser.add_option("--rssDBAddFormulaDescription", action='store', dest="rssDBAddFormulaDescription",
                        help=_("Optional description of the formula"))

    parser.add_option("--rssDBAddFormulaLinkBaseString", action='store', dest="rssDBAddFormulaLinkBaseString",
                        help=_("Sting representing a valid formula linkbase"))
    
    parser.add_option("--rssDBAddFormulaReplaceExistingFormula", action='store_true', dest="rssDBAddFormulaReplaceExistingFormula", default=False,
                        help=_("When this flag is set, If entered formula id in 'rssDBAddFormulaFormulaId' exists in db, it will be updated"))
    
    # Store search results into XBRL DB
    parser.add_option("--rssDBStoreSearchResultsIntoXBRLDB", action='store', dest="rssDBStoreSearchResultsIntoXBRLDB",
                        help=_("Enter Connection paramaters for xbrlDB database to store search results (if any). Connection params are comma separated string as follows: "
                                "host,port,user,password,database[,timeout[,{'postgres|mssqlSemantic|mysqlSemantic|orclSemantic|pgSemantic|sqliteSemantic|pgOpenDB|sqliteDpmDB|rexster|rdfDB|json'}]]"))

def utilityRun(cntlr, options, **kwargs):
    gettext.install('arelle')
    # pass options to cntlr
    # print('rssDB utility run now!!')
    # cntlr.config.setdefault("rssQueryResultsActions", {})
    # cntlr.rssQueryResultsActions = cntlr.config.get("rssQueryResultsActions")

    if getattr(options, 'rssDBFormulaRemoveDups', False):
        cntlr.rssDBFormulaRemoveDups = options.rssDBFormulaRemoveDups
    
    if not hasattr(cntlr, 'userAppTempDir'):
        cntlr.userAppTempDir = os.path.join(cntlr.userAppDir, 'temps')
        if not os.path.exists(cntlr.userAppTempDir):
            os.mkdir(cntlr.userAppTempDir)

    def cleanTemps(dir):
        #clean up
        for f in os.listdir(dir):
            f_path = os.path.join(dir, f)
            os.remove(f_path)

    if not getattr(cntlr, 'atExitAdded', False):
        atexit.register(cleanTemps, cntlr.userAppTempDir)
        cntlr.atExitAdded = True
    
    if options.arellepyRunFormula:
        if options.arellepyRunFormulaFromDB:
            cntlr.addToLog(_('Only one of  "--arellepyRunFormulaFromDB" or "--arellepyRunFormula" can be chosen'),
                    messageCode="arellepy.Error", file=__name__,  level=logging.ERROR)
            raise Exception(_('Only one of  "--arellepyRunFormulaFromDB" or "--arellepyRunFormula" can be chosen'))

    if options.rssDBconnect: # initiates rss db connection, everything depends on this
        try:
            from .RssDB import rssDBConnection
        except:
            from rssDB.RssDB import rssDBConnection

        # for opt in dir(options):
        #     if opt.startswith('arellepy') or opt.startswith('rssDB'):
        #         print(opt, ":", getattr(options, opt, 'Nothing!'))
        
        newFormula=dict()
        con = None
        if hasattr(cntlr, 'rssDBcon'):
            cntlr.rssDBcon.close()
        try:
            con = rssDBConnection(cntlr=cntlr, user=options.rssDBuser,password=options.rssDBpassword, host=options.rssDBhost, 
                                port=options.rssDBport, database=options.rssDBdatabase, timeout=options.rssDBtimeout, 
                                product=options.rssDBproduct, schema=options.rssDBschema, createSchema=options.rssDBcreateSchema,
                                createDB=options.rssDBcreateDB )
        except Exception as e:
            cntlr.addToLog(_('Could not connect to db:\n{}').format(str(e)),
                    messageCode="RssDB.Error", file=os.path.basename(options.rssDBdatabase),  level=logging.ERROR)
            return
        
        _dbStats = con.getDbStats()
        dbStats = _dbStats.get('textResult')
        if dbStats:
            cntlr.rssDBcon = con
            l = ['Database Stats ({}) [{}]:'.format(dbStats.get('DatabaseSize', 'size: 0'), datetime.today().strftime("%Y-%m-%d %H:%M:%S"))]
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
                cntlr.addToLog(_l, messageCode="RssDB.Info", file='{}{}{}'.format(options.rssDBhost, '/' if options.rssDBhost else '', os.path.basename(options.rssDBdatabase)),
                                 level=logging.INFO)

        # Update
        if options.rssDBupdate:
            if options.rssDBupdateEnableAuto:
                duration = timedelta(days=options.rssDBupdateEnableAutoDays, hours=options.rssDBupdateEnableAutoHours, minutes=options.rssDBupdateEnableAutoMinutes)
                
                def autoUpdateHelper(cntlr, mainCon, doAllArgs):
                    conKwargs = {k:v for k,v in mainCon.conParams.items() if not k == 'cntlr' }
                    conn = rssDBConnection(cntlr, **conKwargs, createDB=False)
                    mainCon.autoUpdateConnection = conn
                    mainCon.autoUpdateConnection.doAll(**doAllArgs)
                

                updateKwargsDict = {
                    'setAutoUpdate': True, 'duration':duration,
                    'dateFrom': options.rssDBupdateDateFrom,
                     'dateTo': options.rssDBupdateDateTo,
                     'includeLatest': options.rssDBupdateDoNOTGetLatest,
                     'maxWorkers': options.rssDBupdateMaxWorkers
                }

                con.dbUpdateThread = threading.Thread(target=autoUpdateHelper, args=(cntlr, con, updateKwargsDict), daemon=True)
                con.dbUpdateThread.start()
                time.sleep(3) # give time to setup thread and add info msg to cntlr
            else:
                con.doAll(dateFrom=options.rssDBupdateDateFrom, dateTo=options.rssDBupdateDateTo, includeLatest = options.rssDBupdateDoNOTGetLatest, maxWorkers=options.rssDBupdateMaxWorkers)

        if options.rssDBreportlaunch:
            reportLog = logging.getLogger('werkzeug')
            reportLog.setLevel(logging.ERROR)
            con.startDBReport(host=options.rssDBreporthost, port=options.rssDBreportport, debug=options.rssDBreportdebug, 
                                fromDate=options.rssDBreportfromDate, toDate=options.rssDBreporttoDate, threaded=True)
        
        # Add formula
        if options.rssDBAddFormula:
            try:
                newFormula = con.addFormulaToDb(
                    fileName=options.rssDBAddFormulaFileName,
                    formulaId=int(options.rssDBAddFormulaFormulaId) if options.rssDBAddFormulaFormulaId else options.rssDBAddFormulaFormulaId,
                    description=options.rssDBAddFormulaDescription,
                    formulaLinkBaseString=options.rssDBAddFormulaLinkBaseString,
                    replaceExistingFormula=options.rssDBAddFormulaReplaceExistingFormula,
                    returnData=True)
            except Exception as e:
                if con.product == 'postgres':
                    con.rollback()
                cntlr.addToLog(_('Error while adding formula to db:\n{}').format(str(e)), messageCode="RssDB.Error", file=con.conParams.get('database', ''), level=logging.ERROR)

        # Search DB
        if options.rssDBsearch:
            qResult = con.searchFilings(
                dateFrom=options.rssDBsearchdateFrom,
                dateTo=options.rssDBsearchdateTo,
                assignedSic=options.rssDBsearchassignedSic,
                inlineXBRL=options.rssDBsearchinlineXBRL,
                companyName=options.rssDBsearchcompanyName,
                tickerSymbol= options.rssDBsearchtickerSymbol,
                cikNumber=options.rssDBsearchcikNumber,
                formType=options.rssDBsearchformType,
                limit=options.rssDBsearchlimit,
                getFiles=True
            )
            
            if qResult:
                try:
                    from .CommonFunctions import _makeRssFeedLikeXml, runRenderEdgar, initLocalEdgarViewer
                except:
                    from rssDB.CommonFunctions import  _makeRssFeedLikeXml, runRenderEdgar, initLocalEdgarViewer
                
                # TODO: Can also be rendered as one of the views
                resultFile = rssItems = None                      
                resultFile, rssItems = _makeRssFeedLikeXml(conn=con ,dbFilings_dicts=qResult['filings'], dbFiles_dicts=qResult['files'], saveAs=options.rssDBsearchresultFile, returnRssItems=True)
                cntlr.addToLog(_('Search Result saved to {}').format(resultFile) if resultFile else _('Result file not produces!'),
                                 messageCode="RssDB.Info" if resultFile else "RssDB.Error", file=resultFile,  level=logging.INFO if resultFile else logging.ERROR)
                
                if rssItems:
                    con.searchResults = rssItems
                    if options.arellepyRunFormulaFromDB:
                        if options.arellepyRunFormula:
                            cntlr.addToLog(_('Only one of  "--arellepyRunFormulaFromDB" or "--arellepyRunFormula" can be chosen'),
                                            messageCode="RssDB.Error", file=resultFile, level=logging.ERROR)
                        elif options.arellepyRunFormulaId is None:
                            cntlr.addToLog(_('A formula Id to run must be entered for option --arellepyRunFormulaId'),
                                            messageCode="RssDB.Error", file=resultFile, level=logging.ERROR)                            
                        else:
                            from arellepy.CntlrPy import runFormulaFromDBonRssItems
                            cntlr.formulaeResults = defaultdict(dict)
                            try:
                                formulaResults = runFormulaFromDBonRssItems(conn=con, rssItems=rssItems, formulaId=int(options.arellepyRunFormulaId) if options.arellepyRunFormulaId else options.arellepyRunFormulaId,
                                                                            insertResultIntoDb=options.arellepyRunFormulaFromDBInsertResultIntoDb,
                                                                            updateExistingResults=options.arellepyRunFormulaFromDBUpdateExistingResults,
                                                                            saveResultsToFolder=options.arellepyRunFormulaSaveResultsToFolder,
                                                                            folderPath=options.arellepyRunFormulaFolderPath)
                                cntlr.formulaeResults[options.arellepyRunFormulaId] = formulaResults
                            except Exception as e:
                                cntlr.addToLog(_('Error while running formula:\n {}').format(str(e)),
                                            messageCode="RssDB.Error", file=resultFile,  level=logging.ERROR)
                            
                    if options.rssDBSearchrenderEdgarReports:
                        _pluginDir = options.rssDBsearchEdgarRenderPlugins
                        pluginDir = [p.strip() for p in  _pluginDir.split(',')] if _pluginDir else None
                        if options.rssDBsearchEdgarRenderFolder:
                            output = runRenderEdgar(mainCntlr=cntlr, rssItems=rssItems, saveToFolder=options.rssDBsearchEdgarRenderFolder, pluginsDirs=pluginDir)
                            cntlr.addToLog(_('Render results saved to folder {}').format(output),
                                            messageCode="RssDB.Info", file=resultFile,  level=logging.INFO)
                            if options.rssDBsearchEdgarViewer:
                                initLocalEdgarViewer(cntlr=cntlr, lookinFolder=output, edgarDir=pluginDir[1] if pluginDir else None, threaded=True)
                        else:
                            cntlr.addToLog(_('Output folder for rendered edgar reports must be provided "--rssDBsearchEdgarRenderFolder"'),
                                            messageCode="RssDB.Error", file=resultFile,  level=logging.ERROR)
                    if options.rssDBStoreSearchResultsIntoXBRLDB:
                        from .CommonFunctions import storeInToXbrlDB
                        try:
                            for _i in rssItems:
                                _i.status = None
                                _i.results = []
                            storeInToXbrlDB(cntlr=cntlr,rssItems= rssItems,params=options.rssDBStoreSearchResultsIntoXBRLDB)
                        except Exception as e:
                            cntlr.addToLog(str(e), messageCode="RssDB.Error",  level=logging.ERROR)
        if options.arellepyRunFormula:
            itemsToProcess = []
            _runFormula=True
            if options.arellepyRunFormulaInstancesUrls is None or len(options.arellepyRunFormulaInstancesUrls)==0:
                _runFormula = False
                cntlr.addToLog(_('No urls or search results to Run Fromula'), messageCode="arellepy.Error", level=logging.ERROR)

            if _runFormula and options.arellepyRunFormulaInstancesUrls.upper() == 'FROM_SEARCH':
                if hasattr(con, 'searchResults'):
                    itemsToProcess = con.searchResults
                else:
                    cntlr.addToLog(_('No Search Results to Run Fromula'), messageCode="RssDB.Info",
                                        file=con.conParams.get('dabase', '') if con else '', level=logging.INFO)
                    _runFormula = False
    
            elif _runFormula:
                itemsToProcess = options.arellepyRunFormulaInstancesUrls.split('|')

            if _runFormula:
                if not hasattr(cntlr, 'formulaResults'):
                    cntlr.formulaeResults = defaultdict(dict)
                try:
                    from arellepy.CntlrPy import runFormula
                    # print('ITEMS TO PROCESS', itemsToProcess)
                    # writeFormulaToSourceFile=False, saveResultsToFolder=False, folderPath=None
                    runFormulaResults = runFormula(cntlr=cntlr, instancesUrls=itemsToProcess,
                                                formulaString = options.arellepyRunFormulaString,
                                                formulaSourceFile = options.arellepyRunFormulaSourceFile,
                                                formulaId=int(options.arellepyRunFormulaId) if options.arellepyRunFormulaId else options.arellepyRunFormulaId,
                                                writeFormulaToSourceFile = options.arellepyRunFormulaWriteFormulaToSourceFile,
                                                saveResultsToFolder=options.arellepyRunFormulaSaveResultsToFolder,
                                                folderPath=options.arellepyRunFormulaFolderPath)
                    dictKey = options.arellepyRunFormulaId if options.arellepyRunFormulaId else 'FormulaRun_on_' + datetime.now().strftime("%Y%m%d%H%M")
                    cntlr.formulaeResults[dictKey] = runFormulaResults
                except Exception as e:
                    cntlr.addToLog(_('Error while running formula:\n {}').format(str(e)),
                                messageCode="arellepy.Error", level=logging.ERROR)

        qMsg = [] 
        qInstruction = ['q to quit']      
        dbDash = None
        edgarV = None
        autoUpdateThread = None
        if hasattr(con, 'landingPage'):
            dbDash = con.landingPage[1]
            qMsg.append('DB report running on {}'.format(dbDash))
        if hasattr(cntlr, 'edgarViewerProcess'):
            edgarV = cntlr.edgarViewerProcess[1]
            qMsg.append('Edgar Viewer is running on {}'.format(edgarV))
        if hasattr(con, 'dbUpdateThread'):
            autoUpdateThread = con.dbUpdateThread
            qMsg.append(con.cntlr.autoUpdateINFO)
            qInstruction.insert(0, '0 to stop auto-update')
        
        
        if any([dbDash, edgarV, autoUpdateThread]):
            # msg1 = 'Edgar Viewer is running on {}'.format(edgarV) if edgarV else ''
            # sep = '\n' if edgarV and dbDash else ''
            # sep2 = '\n' if edgarV or dbDash else ''
            # msg2 = 'DB report running on {}'.format(dbDash) if dbDash else ''
            qMe = True
            time.sleep(1)
            while qMe:
                # cmd = input('{}{}{}{}Enter q to quit: '.format(msg1, sep, msg2, sep2))
                cmd = input('\n\n\n{}\nEnter {}: '.format('\n'.join(qMsg), ', '.join(qInstruction)))
                if cmd in ['q', '0']:
                    if autoUpdateThread and not con.autoUpdateConnection.updateStopped:
                        con.autoUpdateConnection.autoUpdateSet = False
                        # wait for current update to end
                        if con.cntlr.autoUpdateINFO in qMsg:
                            qMsg.remove(con.cntlr.autoUpdateINFO)
                        if '0 to stop auto-update' in qInstruction:
                            qInstruction.remove('0 to stop auto-update')
                        con.showStatus(_('Stopping Auto-update...'))
                        while not con.autoUpdateConnection.updateStopped:
                            time.sleep(1)
                if cmd == 'q':
                    qMe = False

def ValidateFormulaFinished(val):
    # Removes duplicates from from formula output and adds some identifying information to be stored with the output
    cntlr = val.modelXbrl.modelManager.cntlr
    if getattr(cntlr, 'rssDBFormulaRemoveDups', False):
        from arellepy.CntlrPy import removeDuplicatesFromXmlDocument
        formulaOutputInstance = val.modelXbrl.formulaOutputInstance
        removeDuplicatesFromXmlDocument(formulaOutputInstance)

def dummyFunc(*args, **kwargs):
    # dummy for class method CntlrWinMain.Menu to force arelle to restart
    pass



__pluginInfo__ = {
    'name': 'SEC XBRL RSS Feeds Database',
    'version': '0.01',
    'description': "This plug-in collects and stores SEC RSS XBRL feeds.  ",
    'license': 'Apache-2 (Arelle plug-in, pymongo), BSD license (pg8000 library), Apache License',
    'author': 'Sherif ElGamal',
    'copyright':' uses: xbrlDB (c) Copyright 2013 Mark V Systems Limited, All rights reserved,\n'
                '      pg8000, Copyright (c) 2007-2009, Mathieu Fenniak (Postgres DB), \n'
                '      pymongo, Author: Mike Dirolf, License: Apache Software License (Apache License, Version 2.0)',
    'CntlrWinMain.Toolbar': rssDBToolBarExtender,
    'CntlrCmdLine.Options': rssDBCmdLineOptionExtender,
    'ValidateFormula.Finished': ValidateFormulaFinished, 
    'CntlrCmdLine.Utility.Run': utilityRun,
    'CntlrWinMain.Menu': dummyFunc,
    'import': ('arellepy', 'xbrlDB')
}
