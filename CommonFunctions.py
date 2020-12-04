'''rssDB Functions dealing with data

Functions included with this module are used to deal with data outside
the database and are independent of the database type, still the database
type has to be one of the types implemented in this plugin.

'''

import sys, os, logging, gettext, time, traceback, csv, pickle, json, gc, re
import concurrent.futures, threading
from datetime import datetime, timedelta
from dateutil import parser, tz
from collections import OrderedDict
from urllib import request
from calendar import monthrange
from lxml import html, etree
from .Constants import rssTables, getTablesFuncs, pathToSQL, rssCols, RSSFEEDS, stateCodes, wait_duration, pathToTemplates
from arelle.UrlUtil import parseRfcDatetime
from arelle import XmlUtil

try:
    from arellepy.HelperFuncs import chkToList, convert_size, xmlFileFromString
    from arellepy.CntlrPy import CntlrPy, subProcessCntlrPy, renderEdgarReportsFromRssItems
    from arellepy.LocalViewerStandalone import initViewer
except:
    from .arellepy.HelperFuncs import chkToList, convert_size, xmlFileFromString
    from .arellepy.CntlrPy import CntlrPy, subProcessCntlrPy, renderEdgarReportsFromRssItems
    from .arellepy.LocalViewerStandalone import initViewer


def _startDBReport(conn, host='0.0.0.0', port=None, debug=False, asDaemon=True, fromDate=None, toDate=None, threaded=True):
    gettext.install('arelle')
    # check if required packages exists
    # first make sure that additional path entries in config file are added
    hasDash = False
    rssDBaddToSysPath = conn.cntlr.config.setdefault('rssDBaddToSysPath', [])
    for p in rssDBaddToSysPath:
        if not p in sys.path:
            sys.path.append(p)
    try:
        import pandas, dash, plotly
        hasDash = True
    except Exception as e:
        hasDash = False        
    dashPath = []
    fail_msg = _("No path was provided to pandas, plotly, dash installation")
    fail_msg_2 = _('One or more of pandas, plotly, dash packages needed to display report is/are not avaliable in the provided path(s)')
    if not hasDash:
        if conn.cntlr.hasGui:
            from tkinter import filedialog
            from tkinter import messagebox
            getpath = messagebox.askyesno(
                title=(_('RSS DB Connection Error')),
                message = (_('pandas, plotly and dash packages needed to display report are not avaliable '
                        'in the current installation of arelle.\n\n'
                        'Do you want the path to an existing intsallation of these packages? Must be for python {}.{}')).format(sys.version_info.major, sys.version_info.minor),
                        icon='warning', parent=conn.cntlr.parent)
            if getpath:
                dashPath = filedialog.askdirectory(title=_('Select pandas, dash, plotly packages installation dir'), parent=conn.cntlr.parent)
                if dashPath and os.path.isdir(dashPath):
                    if not dashPath in sys.path:
                        sys.path.append(dashPath)
                else:
                    conn.cntlr.addToLog(fail_msg, messageCode="RssDB.Info", file="",  level=logging.INFO)
                    messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg, parent=conn.cntlr.parent)
                    return
            else:
                conn.cntlr.addToLog(fail_msg, messageCode="RssDB.Info", file="",  level=logging.INFO)
                messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg, parent=conn.cntlr.parent)
                return
            try:
                import pandas, dash, plotly
                hasDash = True
                _dashPath = chkToList(dashPath, str)
                for p in _dashPath:
                    if p not in rssDBaddToSysPath:
                        rssDBaddToSysPath.append(dashPath)
                conn.cntlr.saveConfig()
            except Exception as e:
                hasDash = False
                conn.cntlr.addToLog(fail_msg_2 +'\n'+str(e) + '\n' + traceback.format_exc(), messageCode="RssDB.Error", file="",  level=logging.ERROR)
                messagebox.showinfo(title=_("RSS DB Info"), message=fail_msg_2 +'\n'+str(e), parent=conn.cntlr.parent)
        else:
            conn.cntlr.addToLog(fail_msg_2, messageCode="RssDB.Error", file="", level=logging.ERROR)
            getPath = input(_('pandas, plotly and dash packages needed to display report are not avaliable '
                        'in the current installation of arelle.\n'
                        'Do you want the path to an existing intsallation of these packages - Must be for python {}.{}? (y/n): ').format(sys.version_info.major, sys.version_info.minor))
            if getPath.lower() == 'y':
                dashPath = input('Input valid path to pandas, plotly, dash installation (probably a site-packages path): ')
                if os.path.isdir(dashPath):
                    sys.path.append(dashPath)
                    try:
                        import pandas, dash, plotly
                        hasDash = True
                        _dashPath = chkToList(dashPath, str)
                        for p in _dashPath:
                            if p not in rssDBaddToSysPath:
                                rssDBaddToSysPath.append(dashPath)
                        conn.cntlr.saveConfig()
                    except Exception as e:
                        hasDash = False
                        conn.cntlr.addToLog(fail_msg_2 +'\n'+str(e) + '\n' + traceback.format_exc(), messageCode="RssDB.Error", file="",  level=logging.ERROR)
                        conn.cntlr.showStatus(fail_msg_2 +'\n'+str(e))             
                        conn.cntlr.showStatus(_('Install required packages or add installation path to "sys.path" before starting report'))
                        return
                else:
                    conn.cntlr.showStatus(_('Install required packages or add installation path to "sys.path" before starting report'))
                    return
            else:
                conn.cntlr.showStatus(_('Aborted'))
                return
    try:
        from .LocalDbDash import RssDBDash
    except:
        try:
            from rssDB.LocalDbDash import RssDBDash
        except:
            from plugin.rssDB.LocalDbDash import RssDBDash
    
    reportLog = logging.getLogger('werkzeug')
    reportLog.setLevel(logging.ERROR)
    conn.dashboard = RssDBDash(conn, fromDate=fromDate, toDate=toDate)
    conn.landingPage = conn.dashboard.startDash(host, port, debug, asDaemon, threaded)
    return conn.landingPage

def updateCikTickerMapping(conn, createTable=False, returnStats=False):
    startTime = time.perf_counter()
    url = 'https://www.sec.gov/include/ticker.txt'
    tableExists = rssTables[5] in getTablesFuncs[conn.product](conn)
    result = None
    _stat = None
    if createTable:
        if conn.product in ['sqlite', 'postgres']:
            conn.execute('DROP TABLE IF EXISTS {}'.format(rssTables[5]), fetch=False)
            conn.execute('''CREATE TABLE IF NOT EXISTS "cikTickerMapping" (
                            "cikNumber" TEXT,
                            "tickerSymbol" TEXT
                            )''', fetch=False)
        elif conn.product == 'mongodb':
            conn.dbConn[rssTables[5]].drop()
            conn.dbConn.create_collection(rssTables[5])
        tableExists = rssTables[5] in getTablesFuncs[conn.product](conn)
    if not tableExists:
        conn.addToLog(_('{} {} was not created').format(rssTables[5], 'table' if conn.product in ['sqlite', 'postgres'] else 'collection'), 
                            messageCode="RssDB.Info", messageArgs=conn.conParams.get('database',''), file="",  level=logging.INFO)
        _stat = _('{} not updated').format(rssTables[5])
    else:
        if conn.product in ['sqlite', 'postgres']:
            conn.execute('DELETE FROM "{}"'.format(rssTables[5]), fetch=False)
        elif conn.product == 'mongodb':
            conn.dbConn[rssTables[5]].remove({})

        resp = request.urlopen(url)
        if resp.code == 200:
            resp_lines = [x.decode() for x in resp.readlines()]
            lines = {tuple(l) for l in list(csv.reader(resp_lines, delimiter='\t'))} # remove dups
            data = [{'tickerSymbol':x[0], 'cikNumber': x[1].zfill(10)} for x in lines if x[0]]
            _stat = conn.insertUpdateRssDB(data, "cikTickerMapping", returnStat=True)
            if conn.product in ['sqlite', 'postgres']:
                conn.commit()            
        else:
            conn.addToLog('Could not get data, {} returned code {}'.format(url, resp.code), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    totalTime = round(time.perf_counter() - startTime, 3)
    _msg = 'Finished updating {} in {} secs'.format(rssTables[5], totalTime)
    conn.addToLog(_msg, messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    if returnStats:
        result = {'stats': _msg}
        if _stat:
            result['summary'] = _stat
    return result

def _populateFilersInfo(conn):
    fileName = os.path.join(pathToSQL, '{}_filersInfo.pkl'.format(conn.product))
    flag=False
    try:
        with open(fileName, 'rb') as target:
            filersInfo = pickle.load(target)
        fileDate = filersInfo.get('retrivedOn', '"NA"')
        fileSource= filersInfo.get('sourceDBType', '"NA"')
        conn.cntlr.addToLog(_("Populating filersInfo from data stored on {} retrived from db of type {}").format(fileDate, fileSource), messageCode="RssDB.Info", file=conn.conParams.get('database', ''), level=logging.INFO)
        data = filersInfo['data']
        if conn.product == "postgres":
            # no json for formerNames
            for d in data:
                if not isinstance(d['formerNames'], str):
                    d['formerNames'] = json.dumps(d['formerNames'], default=lambda x: str(x)) if not d['formerNames'] is None else d['formerNames']
        flag = True # if we need to rollback or not!
        conn.insertUpdateRssDB(data, rssTables[3])
    except FileNotFoundError:
        conn.cntlr.addToLog(_("File containing stored filersInfo not found at {}").format(fileName), messageCode="RssDB.Error", file=conn.conParams.get('database', ''), level=logging.ERROR)
    except Exception as e:
        conn.cntlr.addToLog(_("Error while populating filersInfo:\n{}").format(str(e)), messageCode="RssDB.Error", file=conn.conParams.get('database', ''), level=logging.ERROR)
        if conn.product == 'postgres' and flag:
            conn.rollback()

def _getLastBuild(fPath, feedMonth):
    """helper function for concurrent executor parses feed xml for lastbuildDate"""
    from lxml import etree
    from dateutil import parser
    fd = dict()
    _lastBuildDate = etree.parse(fPath).getroot().xpath('.//channel/lastBuildDate/text()')
    lastBuildDate = parser.parse(_lastBuildDate[0], tzinfos={'EST':'UTC-5:00', 'EDT':'UTC-4:00'})
    feedMonth_date = parser.parse(feedMonth)
    fd['feedId'] = int(feedMonth.replace("-", ""))
    fd['feedDate'] = feedMonth_date.replace(day=monthrange(year=feedMonth_date.year, month=feedMonth_date.month)[1])
    fd['link'] = fPath
    fd['lastBuildDate'] = lastBuildDate
    return fd

def _getFeedInfo(conn, link, lastModifiedDate, isNew, reloadCache=False):
    """Gets feed info ready to insert in db"""
    startAllTime = time.perf_counter()
    # always reload cache for modified feeds otherwise reload when reloadCache is specified
    if not isNew or (isNew and reloadCache):
        conn.showStatus(_('Updating cached {}').format(link))
        # account for multiple processes trying to create same cache folder when cache is cleared
        while True:
            try:
                gettext.install('arelle')
                conn.cntlr.webCache.getfilename(link, reload=True)
            except FileExistsError as e:
                time.sleep(.5)
                continue
            break
    mdlXbrl = None
    while not mdlXbrl:
        conn.cntlr.runKwargs(file=link, keepOpen='')
        mdlXbrl = conn.cntlr.modelManager.modelXbrl
    modelDoc = mdlXbrl.modelDocument
    conn.showStatus(_("Getting feed info from {}").format(link))
    feedInfo = OrderedDict.fromkeys(rssCols[rssTables[0]])
    fileId_re = re.compile(r"\d{4}-\d{2}")
    fileMonth = None
    if modelDoc.basename == os.path.basename(RSSFEEDS['US SEC All Filings']):
        _fileMonth = parseRfcDatetime(modelDoc.xmlRootElement.xpath('./channel/lastBuildDate')[0].textValue)
        fileMonth = datetime.strftime(_fileMonth, '%Y-%m')
    elif modelDoc.basename:
        fileMonth = fileId_re.search(modelDoc.basename).group()
    feedInfo['feedId'] = int(fileMonth.replace('-', ''))
    fileMonth_date = parser.parse(fileMonth)
    feedInfo['feedMonth'] = fileMonth_date.replace(
        day=monthrange(fileMonth_date.year, fileMonth_date.month)[1]
    )
    feedInfoObjects = modelDoc.xmlDocument.xpath('.//channel/*[not(self::item)]')
    for inf in feedInfoObjects:
        tag = str(inf.qname)
        val = None
        if tag.startswith('atom:'):
            val = inf.attr('href')
            tag = 'feedLink'
        else:
            val = inf.text
        feedInfo[tag] = val
    try:
        feedInfo['pubDate'] = parser.parse(feedInfo.get('pubDate'), tzinfos={'EST':'UTC-5:00', 'EDT':'UTC-4:00' })
        feedInfo['lastBuildDate'] = parser.parse(feedInfo.get('lastBuildDate'), tzinfos={'EST':'UTC-5:00', 'EDT':'UTC-4:00' })
    except:
        pass

    feedInfo['lastModifiedDate'] = lastModifiedDate
    conn.showStatus(_("Getting feed items from {}").format(link))
    modelDoc.rssItems.reverse()
    _rssItemsList = modelDoc.rssItems
    if not isNew:
        doc_accessions = modelDoc.xmlDocument.xpath('.//*[local-name()="accessionNumber"]/text()')
        db_accessions = []

        if not isNew:
            if conn.product in ['sqlite', 'postgres'] and rssTables[1] in conn.tablesInDB():
                _qry = 'SELECT "accessionNumber" FROM "{}" where "feedId"={}'.format(rssTables[1], feedInfo['feedId'])
                try:
                    _db = conn.execute(_qry, close = False)
                    db_accessions = [x[0] for x in _db]
                except Exception as e:
                    conn.rollback()
                    raise e
            elif conn.product == 'mongodb' and rssTables[1] in conn.dbConn.list_collection_names():
                _db = list(conn.dbConn[rssTables[1]].find({"feedId":feedInfo['feedId']}, {"accessionNumber":1, "_id":0}))
                db_accessions = [x['accessionNumber'] for x in _db]
        
        _new_accessions = [x for x in doc_accessions if x not in db_accessions]
        _rssItemsList = [x for x in modelDoc.rssItems if x.accessionNumber in _new_accessions]
    return feedInfo, _rssItemsList

def getFilesInfo(modelRssItem, feedId, filingId):
    """Gets files information from modelRssItems ready to be inserted in db
    (files included in the filing)"""
    _i = modelRssItem
    filesInfoList = []
    itemFiles = XmlUtil.descendants(_i, _i.edgr, 'xbrlFile')
    for _f in itemFiles:
        filesInfoDict = OrderedDict.fromkeys(rssCols['filesInfo'])
        for _t in _f.elementAttributesTuple:
            filesInfoDict[_t[0].replace('{'+_i.edgr+'}', '')] = _t[1]
        filesInfoDict['feedId'] = feedId
        filesInfoDict['filingId'] = filingId
        filesInfoDict['duplicate'] = 0
        filesInfoDict['accessionNumber'] = getattr(_i,'accessionNumber', None)
        filesInfoDict['fileId'] = int(str(filingId) + str(filesInfoDict['sequence']).zfill(3))
        if not filesInfoDict.get('inlineXBRL'):
            filesInfoDict['inlineXBRL'] = False
        filesInfoDict['sequence'] = int(filesInfoDict['sequence'])
        filesInfoDict['size'] = int(filesInfoDict['size']) if filesInfoDict['size'] else None
        
        if isinstance(filesInfoDict['inlineXBRL'], bool):
            filesInfoDict['inlineXBRL'] = int(filesInfoDict['inlineXBRL'])
        elif isinstance(filesInfoDict['inlineXBRL'], str):
            if 't' in filesInfoDict['inlineXBRL'].lower():
                filesInfoDict['inlineXBRL'] = 1
            elif 'f' in filesInfoDict['inlineXBRL'].lower():
                filesInfoDict['inlineXBRL'] = 0
        
        tags_dict = {
            'ins': 'INS',
            'sch': 'SCH',
            'cal': 'CAL',
            'def': 'DEF',
            'lab': 'LAB',
            'pre': 'PRE'
        }
        
        if filesInfoDict.get('type'):
            filesInfoDict['type_tag'] = tags_dict.get(filesInfoDict.get('type')[-3:].lower())
        if not filesInfoDict['type_tag']:
            filesInfoDict['type_tag'] = 'INS' if filesInfoDict['inlineXBRL'] else 'OTHER'

        filesInfoList.append(filesInfoDict)
    return filesInfoList

def _getMonthlyFeedsLinks(conn, loc=None, maxWorkers=None, dateFrom=None, dateTo=None, last=None, _log=False):
    """Returns Monthly feeds Links  that are not in DB or with lastModified date later than in DB"""
    if not maxWorkers:
        maxWorkers = os.cpu_count()/2
    if not loc:
        loc = 'https://www.sec.gov/Archives/edgar/monthly/'
    rssPattern = re.compile(r'^xbrlrss-\d{4}-\d{2}.xml*')
    urlPattern = re.compile(r'^(http:|https:)')
    yearMonPattern = re.compile(r"\d{4}-\d{2}")
    feeds = []
    compareCol = 'lastModifiedDate'
    startTime = time.perf_counter()
    # For testing on local files only
    if os.path.isdir(loc):
        # Reduce time and keep lxml memory usage in subprocesses, not for windows though!
        with concurrent.futures.ProcessPoolExecutor(max_workers=maxWorkers) as executor: 
            a1 = [os.path.join(p,_f) for p,d,f in list(os.walk(loc)) for _f in f if rssPattern.match(_f)]
            a2 = [yearMonPattern.search(_f).group() for _f in a1]
            _feeds = [executor.submit(_getLastBuild, _a1, _a2) for _a1, _a2 in zip(a1, a2)]
            for _fd in concurrent.futures.as_completed(_feeds):
                feeds.append(_fd.result())
                if _log:
                    conn.addToLog(_('Found -- {}').format(_fd.result()['link']), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
        compareCol = 'lastBuildDate'
    elif urlPattern.match(loc):
        feedsPage = None
        try:
            feedsPage = request.urlopen(loc)
            if feedsPage.code == 200:
                conn.showStatus(_('Getting feeds info from {}').format(loc))
                tree = html.parse(feedsPage).getroot().xpath('.//table//tr[child::td]')
                href = [(x.xpath('td/a/@href')[0], parser.parse(x.xpath('td[3]/text()')[0])) for x in tree]
                for h in href:
                    if rssPattern.match(h[0]):
                        fd = dict()
                        feedMonth = yearMonPattern.search(h[0]).group()
                        feedMonth_date = parser.parse(feedMonth)
                        fd['feedId'] = int(feedMonth.replace("-", ""))
                        fd['feedDate'] = feedMonth_date.replace(day=monthrange(year=feedMonth_date.year, month=feedMonth_date.month)[1])
                        fd['link'] = loc + h[0]
                        fd['lastModifiedDate'] = h[1]
                        feeds.append(fd)
                        if _log:
                            conn.addToLog(_('Found -- {}').format(fd['link']), messageCode="RssDB.Info", file=conn.conParams.get('database',''), level=logging.INFO)
            else:
                conn.addToLog(_('{} returned code {}').format(loc, feedsPage.code), messageCode="RssDB.Info", file=conn.conParams.get('database',''), level=logging.INFO)
        except Exception as e:
            conn.addToLog(_('Error while getting feeds:\n{}').format(str(e)), messageCode="RssDB.Error", file=conn.conParams.get('database',''), level=logging.ERROR)
    else:
        raise ValueError('Don''t know what to do with {}'.format(loc))
    
    return feeds, compareCol, startTime

def getRssItemInfo(modelRssItem, feedId, filingId, getFiles=True, getXML=False):
    """Gets filingInfo info ready to insert in db"""
    _i = modelRssItem
    itemInfoDict = OrderedDict.fromkeys(rssCols[rssTables[1]])
    itemInfoDict['inlineXBRL'] = 0
    itemInfoDict['duplicate'] = 0
    inlineAttrib = _i.xpath('.//@*[local-name()="inlineXBRL"]')
    if inlineAttrib:
        isInlineXbrl = inlineAttrib[0]
        if isinstance(isInlineXbrl, str):        
            if 't' in isInlineXbrl.lower():
                itemInfoDict['inlineXBRL'] = 1
            else:
                itemInfoDict['inlineXBRL'] = 0
        elif isinstance(isInlineXbrl, bool):
            int(isInlineXbrl)
        else:
            itemInfoDict['inlineXBRL'] = 0
    itemInfoDict['feedId'] = feedId
    itemInfoDict['filingId'] = filingId
    itemInfoDict['filingLink'] = _i.find('link').text
    _attribs = [
        'enclosureUrl', 'enclosureSize', 'url', 'pubDate', 'companyName', 'formType',
        'filingDate', 'cikNumber', 'accessionNumber', 'fileNumber',
        'acceptanceDatetime', 'period', 'assignedSic', 'fiscalYearEnd'
    ]
    for _attr in _attribs:
        if _attr == 'url':
            itemInfoDict['entryPoint'] = getattr(_i, _attr, None)
        elif _attr == 'enclosureSize':
            itemInfoDict[_attr] = int(_i.find('enclosure').get('length')) if _i.find('enclosure') is not None else None
        else:
            itemInfoDict[_attr] = getattr(_i, _attr, None)
    if itemInfoDict['period']:
        _period = parser.parse(itemInfoDict['period'])
        itemInfoDict['period'] = _period
    try:
        _cnvrtDate = datetime.combine(itemInfoDict['filingDate'], datetime.min.time())
        itemInfoDict['filingDate'] = _cnvrtDate
        if not itemInfoDict['assignedSic']:
            itemInfoDict['assignedSic'] = 0
        else:
            itemInfoDict['assignedSic'] = int(itemInfoDict['assignedSic'])
        
        itemInfoDict['assistantDirector'] = _i.xpath('.//*[local-name()="assistantDirector"]/text()')[
            0] if _i.xpath('.//*[local-name()="assistantDirector"]/text()') else None

        if itemInfoDict['fiscalYearEnd']:
            monthDay = itemInfoDict['fiscalYearEnd'].split('-')
            itemInfoDict['fiscalYearEndMonth'] = int(monthDay[0])
            itemInfoDict['fiscalYearEndDay'] = int(monthDay[1])
    except:
        pass
    filesInfo = None
    rssXml = None
    result = {
        rssTables[1]:itemInfoDict
    }
    if getFiles:
        filesInfo = getFilesInfo(modelRssItem, feedId, filingId)
        if len(filesInfo)>0:
            result[rssTables[2]] = filesInfo
    
    if getXML:
        rssXml = OrderedDict.fromkeys(rssCols[rssTables[4]])
        rssXml['filingId'] = filingId
        rssXml['rssItem'] = etree.tostring(modelRssItem).decode(modelRssItem.document.xmlDocument.docinfo.encoding)
        result[rssTables[4]] = rssXml
    return result

def getFilerInformation(conn, ciks:list, timeOut=5, retries=3, i=None, _all=None, maxWorkers=4):
    if maxWorkers:
        try:
            maxWorkers = int(maxWorkers)
        except:
            maxWorkers = 4
        if maxWorkers > 4:
            # use max of 4 processes to avoid SEC site blocking connection
            maxWorkers = 4
        
    result = _getFilerInformation(conn=conn, ciks=ciks, i=i, _all=_all, timeOut=timeOut, maxWorkers=maxWorkers)
    if retries and len(result['missing']) > 0:
        missing_after_retry = result['missing']
        i = retries
        _i = 1
        while i > 0:
            if len(missing_after_retry) > 0:
                conn.showStatus(_('Trying to get missing ciks {} of {}').format(_i, retries), 2000, end='\r')
                missing_retry = _getFilerInformation(conn=conn, ciks=missing_after_retry, timeOut=timeOut, maxWorkers=maxWorkers)
                if len(missing_retry['retrived']) > 0:
                    result['retrived'].extend(missing_retry['retrived'])
                if len(missing_retry['missing']) > 0:
                    result['missing'] = missing_retry['missing']
                    missing_after_retry = missing_retry['missing']
                if len(missing_retry['missing']) == 0:
                    result['missing'] = missing_retry['missing']
                    break
                i -=1
                _i +=1
    return result

def _getFilerInformation(conn, ciks:list, i=None, _all=None, timeOut=5, maxWorkers=4):
    try:
        maxWorkers = int(maxWorkers)
    except:
        maxWorkers = 4
        
    if maxWorkers > 4:
        # cannot make more than 10 requests per second with sec.gov so max workers should be 4 max
        maxWorkers = 4
    
    ciksLst = chkToList(ciks, str)
    a = time.perf_counter()
    filerInfos = []
    missing = []
    if not i:
        i = 1
    if not _all:
        _all = len(ciksLst)
    if sys.platform.lower().startswith('win'):
        # windows app and multiprocessing issues!
        conn.addToLog(_('Not using multiprocessing to get filers information'), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
        for c in ciksLst:
            res = _filerInformation(c, timeOut=timeOut, dbType=conn.product)
            hasInfo = res.get('filerInfo')
            cik_db = res.get('cik')
            msg = '{}/{} '.format(i, _all)
            if hasInfo:
                filerInfos.append(res)
                msg = msg + 'Retrived cik {} -- {}'.format(cik_db, hasInfo.get('conformedName'))
            else:
                msg = msg + 'Could not retrive cik {}'.format(cik_db)
                missing.append(cik_db)
            conn.showStatus(msg, 2000, end='\r')
            i +=1
            time.sleep(.1) # make sure we dont spam sec.gov
    else:
        conn.addToLog(_('Using multiprocessing to get filers information'), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
        with concurrent.futures.ProcessPoolExecutor(max_workers= maxWorkers if maxWorkers <= 4 else 4) as executor:
            a1 = ciksLst
            a2 = [timeOut] * len(a1)
            a3 = [conn.product] * len(a1)
            _filerInfos = [executor.submit(_filerInformation, _a1, _a2, _a3) for _a1, _a2, _a3 in zip(a1, a2, a3)]
            for _info in concurrent.futures.as_completed(_filerInfos):
                hasInfo = _info.result().get('filerInfo')
                cik_db = _info.result().get('cik')
                msg = '{}/{} '.format(i, _all)
                if hasInfo:
                    filerInfos.append(_info.result())
                    msg = msg + 'Retrived cik {} -- {}'.format(cik_db, hasInfo.get('conformedName'))
                else:
                    msg = msg + 'Could not retrive cik {}'.format(cik_db)
                    missing.append(cik_db)
                conn.showStatus(msg, 2000, end='\r')
                i +=1
    b = time.perf_counter()
    conn.addToLog(_('Finished getting filers information in {} secs').format(round(b-a,3)), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    if len(missing) > 0:
        conn.addToLog(_('Could not retrieve {} cik(s): {}').format(len(missing), missing), messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    return {'retrived':filerInfos, 'missing': missing, 'i':i}

def _filerInformation(cik, timeOut, dbType):
    url = 'https://www.sec.gov/cgi-bin/browse-edgar?CIK={}&action=getcompany&output=atom'.format(cik)
    filerInformation = dict()
    try:
        resp = request.urlopen(url, timeout=timeOut)
        tree = etree.parse(resp)
        root = tree.getroot()
        ns = root.nsmap
        filer = root.find('.//company-info', namespaces=ns)
        
        adderTypes = ['mailing', 'business']
        addrInfo = ['city', 'state', 'zip']
        addresses = []
        for addr in adderTypes:
            _addrEl = filer.find('./addresses/address[@type="{}"]'.format(addr), ns)
            addressValues = [_addrEl.find('./{}'.format(x), ns).text if _addrEl.find(
                './{}'.format(x), ns) is not None else None for x in addrInfo]
            addressKeys = [addr + x.capitalize() for x in addrInfo]
            addresses.append(dict(zip(addressKeys, addressValues)))
        addressesAll = {**addresses[0], **addresses[1]}
        
        formerNamesEl = filer.find('./formerly-names', ns)
        formerNames = []
        if formerNamesEl is not None:
            _formerNames = [{'name': x.text, 'date': parser.parse(y.text)} for x,y in zip(
                [x for x in formerNamesEl.findall('.//name', ns)],
                [x for x in formerNamesEl.findall('.//date', ns)]
            )]
            # last name change first
            _formerNames_sorted = sorted(_formerNames, key=lambda x: x['date'], reverse=True)
            if dbType in ['sqlite', 'postgres']:
                formerNames = json.dumps(_formerNames_sorted, default= lambda x:str(x))
            elif dbType == 'mongodb':
                formerNames = _formerNames_sorted

        filerInformation = {
            'conformedName': filer.find('./conformed-name', ns).text if filer.find('./conformed-name', ns) is not None else None,
            'cikNumber': filer.find('./cik', ns).text if filer.find('./cik', ns) is not None else None,
            'industry_code': filer.find('./assigned-sic', ns).text if filer.find('./assigned-sic', ns) is not None else None,
            'industry_description': filer.find('./assigned-sic-desc', ns).text if filer.find('./assigned-sic-desc', ns) is not None else None,
            'stateOfIncorporation': filer.find('./state-of-incorporation', ns).text.strip() if filer.find('.//state-of-incorporation', ns) is not None else None,
            'country': None,
            **addresses[0], **addresses[1], 'formerNames': formerNames if formerNames else None}
        state = filerInformation['businessState'] or filerInformation['mailingState']
        if state is not None:
            filerInformation['country'] =  stateCodes.get(state.upper())[0]
    except Exception:
        pass
    return {'filerInfo' : filerInformation, 'cik': cik }

def _xDoAll(conn, loc=None, last=None, dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, 
            maxWorkers=None, updateDB=True, reloadCache=False, updateExisting=True, refreshAll=False, 
            timeOut=3, retries=3, includeLatest=True, getFiles=True, getXML=False, getFilers=True, updateTickers=True, q=None):
    '''Creates and populates rssDB or jus updates db if db exists 

    args:  

        conn: rss db connection
        loc: location of rss feed files if stored locally (for testing only)
        last {int}: number of feeds to retrive from the last (eg: 3 to get last 3 feeds)
        dateFrom, dateTo: date range in yyyy-mm-dd to retrive feeds within this date range only
        getRssItems: always True (for testing only)
        returnInfo: Return stats of this run as a dict (number of inserts, updates....)
        maxWorkers: max number of processes to use, defaults to half of cpu count on the machine.
        updateDB: whether to insert into db, should be always true
        reloadCache: reload already cached files, should be False always, this is only for testing,
                     noting that last feed is always reloaded.
        updateExisting: check existing filers' information to see if needs update
        refreshAll: reload all filers' information, takes a lot of time.
        timeout: timeout for trying to fetch each filer information.
        retries: number of times to retry getting filer information if it fails.
        includeLatest: whether to include last 10 mins filing available in a seprate feed on SEC site, should be always True
        getFiles: store filing files in db, big table, but useful information.
        getXML: store rssFeed xml for each filing.
        getFilers: get filers data
        updateTickers: update ticker cik mapping from SEC website
        q: multiprocessing.Manager().queue to transfer stat message in multiprocessing
    '''
    conn.updateStarted = True
    startTime = time.perf_counter()
    
    rssFeeds = conn.updateRssFeeds(loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, getRssItems=getRssItems,
                                returnInfo=returnInfo, maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, 
                                includeLatest=includeLatest, getFiles=getFiles, getXML=getXML, q=q)
    filersInfo = None
    if getFilers:
        filersInfo = conn.updateFilersInfo(updateExisting=updateExisting, refreshAll=refreshAll, updateDB=updateDB,
                                        maxWorkers=maxWorkers, timeOut=timeOut, retries=retries, returnData=returnInfo)
    cikTickerMapping = None
    if updateTickers:
        cikTickerMapping = updateCikTickerMapping(conn, returnStats=True)
    updatedOn = parser.parse(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    conn.insertUpdateRssDB({'id': 0, 'lastUpdate': updatedOn},
                            'lastUpdate', 'update', 'lastUpdate', 'id', True, False)
    
    endTime = time.perf_counter()
    
    if filersInfo:
        rssFeeds['summary'][rssTables[3]] = filersInfo['summary'][rssTables[3]]
    if cikTickerMapping:
        if isinstance(cikTickerMapping.get('summary'), dict):
            rssFeeds['summary'][rssTables[5]] = cikTickerMapping['summary']

    dupMsg = None
    if rssFeeds['summary']['feedsInfo']['insert'] > 0:
        dupStat = conn.updateDuplicateFilings()
        rssFeeds['summary']['filingsInfo']['update'] = rssFeeds['summary']['filingsInfo']['update'] + dupStat['filingsInfo']['update']
        rssFeeds['summary']['filesInfo']['update'] = rssFeeds['summary']['filesInfo']['update'] + dupStat['filesInfo']['update']
        dupMsg = dupStat['msg']

    _stats = ['Summary:']
    for k,v in rssFeeds['summary'].items():
        _stats.append('{}: inserts {} -- updates {}'.format(k,v.get('insert', 0),v.get('update', 0)))
    if filersInfo:
        _stats.extend([rssFeeds['stats'],filersInfo['stats']])
        if dupMsg:
            _stats.append(dupMsg)

    rssFeeds['stats'] = _stats[1:]

    for _stat in _stats:
        conn.addToLog(_stat, messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    
    _msg = _('Finished update in {} secs on {}').format(round(endTime - startTime,3), str(updatedOn))
    conn.addToLog(_msg, messageCode="RssDB.Info", file=conn.conParams.get('database',''),  level=logging.INFO)
    conn.showStatus(_msg)
    rssFeeds['stats'].append(_msg)
    results = {'summary': rssFeeds['summary'], 'stats': rssFeeds['stats']}
    if returnInfo:
        results['feeds'] = rssFeeds['feeds'] 
        if filersInfo:
            results['filers'] = filersInfo
    # update filers' dump
    conn.dumpFilersInfo()
    conn.updateStarted = False
    return results

def _doAll(conn, setAutoUpdate=False, waitFor=timedelta(minutes=wait_duration), duration=timedelta(hours=1),
                loc=None, last=None, dateFrom=None, dateTo=None, getRssItems=True, returnInfo=False, 
                maxWorkers=None, updateDB=True, reloadCache=False, updateExisting=True, refreshAll=False, 
                timeOut=3, retries=3, includeLatest=True, getFiles=True, getXML=False, getFilers=True, updateTickers=True, q=None):
    results = None
    if setAutoUpdate:
        conn.updateStopped = False
        if not isinstance(duration, timedelta) and isinstance(waitFor, timedelta):
            conn.cntlr.addToLog(_("waitFor and duration must be an object of class timedelta"), messageCode="RssDB.Error", file=conn.conParams['database'], level=logging.ERROR)
            raise Exception("waitFor and duration must be an object of class timedelta")
        startTime = datetime.now()
        endTime = startTime + duration
        msg = _("Auto-update for db {} started at {} until {}").format(conn.conParams.get('database',''), str(datetime.now().replace(microsecond=0)), str(endTime.replace(microsecond=0)))
        conn.cntlr.autoUpdateINFO = msg
        conn.cntlr.addToLog(msg, messageCode="RssDB.Info", file=conn.conParams['database'], level=logging.INFO)
        conn.autoUpdateSet = True
        while endTime > datetime.now():
            if not conn.autoUpdateSet:
                conn.updateStopped = True
                break
            else:
                try:
                    results = _xDoAll(conn, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, getRssItems=getRssItems, returnInfo=False, # don't return anything
                                        maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, 
                                        refreshAll=refreshAll, timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, 
                                        getXML=getXML, getFilers=getFilers, updateTickers=updateTickers, q=q)
                except Exception as e:
                    conn.cntlr.addToLog(_('Error while updating db:\n{}').format(str(e)), messageCode="RssDB.Error", file=conn.conParams['database'], level=logging.ERROR)
            cycleTime = datetime.now()
            # capture stop signal while waiting for next update
            while cycleTime + waitFor > datetime.now():
                if not conn.autoUpdateSet:
                    conn.updateStopped = True
                    break
                else:
                    time.sleep(2)
        conn.cntlr.addToLog(_("Stopped auto-update at {}").format(str(datetime.now().replace(microsecond=0))), messageCode="RssDB.Info", file=conn.conParams['database'], level=logging.INFO)
    
    else:
        try:
            results = _xDoAll(conn, loc=loc, last=last, dateFrom=dateFrom, dateTo=dateTo, getRssItems=getRssItems, returnInfo=returnInfo,
                                maxWorkers=maxWorkers, updateDB=updateDB, reloadCache=reloadCache, updateExisting=updateExisting, refreshAll=refreshAll, 
                                timeOut=timeOut, retries=retries, includeLatest=includeLatest, getFiles=getFiles, getXML=getXML, getFilers=getFilers, 
                                updateTickers=updateTickers, q=q)
        except Exception as e:
            conn.cntlr.addToLog(_('Error while updating db:\n{}').format(str(e)), messageCode="RssDB.Error", file=conn.conParams['database'], level=logging.ERROR)
    return results

def _makeRssFeedLikeXml(conn, dbFilings_dicts, dbFiles_dicts, saveAs=None, returnRssItems=False, showcount=True):
    '''Create xml document like rss feed that can be loaded to arelle'''
    # prep data for to write XML
    conn.showStatus(_('preparing data'),2000)
    # re-attach files to filing
    for d in dbFilings_dicts:
        d['files'] = [f for f in dbFiles_dicts if f['filingId']==d['filingId']]
        for _k,_v in d.items():
            if not _v:
                d[_k]=''
    # convert values to string   
    for d in dbFilings_dicts:
        d['pubDate'] = parser.parse(d['pubDate']).strftime("%a, %d %b %Y %H:%M:%S %Z") if isinstance(
            d['pubDate'], str) else d['pubDate'].strftime("%a, %d %b %Y %H:%M:%S %Z")
        d['filingDate'] = parser.parse(d['filingDate']).strftime(
            "%m/%d/%Y") if isinstance(d['filingDate'], str) else d['filingDate'].strftime("%m/%d/%Y")
        d['fiscalYearEnd'] = d['fiscalYearEnd'].replace('-', '') if d['fiscalYearEnd'] else ''
        if d.get('period'):
            _period = d['period'] if isinstance(d['period'], str) else str(d['period'])
            d['period'] = str(parser.parse(_period).date()).replace('-', '')
        d['filingId'] = str(d['filingId'])
        for _d in d['files']:
            _d['inlineXBRL'] = "true" if bool(_d['inlineXBRL']) else "false"

    # create rss skelton from template
    timeNow = datetime.now(tz.tzlocal()).strftime("%a, %d %b %Y %H:%M:%S %Z")
    rssTemplate = os.path.join(pathToTemplates, 'rssFeedtmplt.xml')
    
    with open(rssTemplate, 'r') as f:
        rssTmplt = f.read().format(a=timeNow)

    rssDoc = etree.fromstring(rssTmplt)
    rssChannel = rssDoc.find('channel')

    # Create mapping between db cols and xml elements
    filingCols = ['companyName', 'formType', 'filingDate', 'cikNumber', 'accessionNumber', 'fileNumber', 'acceptanceDatetime', 'period', 'assignedSic', 'assistantDirector', 'fiscalYearEnd']
    edgrNsmap = {'edgar': 'https://www.sec.gov/Archives/edgar'}
    edgrPrefix = '{' + edgrNsmap['edgar'] + '}' # '{https://www.sec.gov/Archives/edgar}'
    fileCols = rssCols['filesInfo'][4:]

    # create xml elements from the mappings dicts
    def makeEl(tag, _data, parent):
        attribs = _data.get('attrib') if _data.get('attrib') else {}
        text = _data.get('text')
        child = etree.Element(tag, **attribs , nsmap=_data.get('nsmap'))
        child.text = text
        parent.append(child)
        if 'children' in _data.keys():
            for k, v in _data['children']:
                makeEl(k, v, child)
        return parent
    
    # map database columns to relevent xml elements (for each element text => xml value, attrib => xml attribute, children => sub elements)
    for _xi, dbFiling_i in enumerate(dbFilings_dicts):
        # make xbrlFiles element mapping
        if showcount:
            conn.showStatus(_('making item {}\r').format(_xi+1), 2000, end ='\n' if _xi == len(dbFilings_dicts)-1 else "")
        files_map = [(edgrPrefix+'xbrlFiles',{
            'nsmap' : edgrNsmap,
            'children' : [
                (edgrPrefix+'xbrlFile',{
                    'nsmap': edgrNsmap,
                    'attrib': OrderedDict((edgrPrefix+x, str(f.get(x, ''))) for x in fileCols)
                }) for f in sorted(dbFiling_i['files'], key=lambda x: x['sequence'])
            ]

        })]

        # make item element mapping attaching xbrlFiles element
        xml_db_mapping = [
            ('filingId' , {
                'text': dbFiling_i['filingId']
            }),
            ('isInlineXBRL' , {
                'text': "true" if bool(dbFiling_i['inlineXBRL']) else "false"
            }),
            ('title' , {
                'text': dbFiling_i['companyName']
            }),
            ('link', {
                'text': dbFiling_i['filingLink']
            }),
            ('guid', {
                'text': dbFiling_i['enclosureUrl']
            }),
            ('enclosure', {
                'attrib': {'url': dbFiling_i['enclosureUrl'], 'length':str(dbFiling_i['enclosureSize']), 'type': 'application/zip'}
            }),
            ('description', {
                'text': dbFiling_i['formType']
            }),
            ('pubDate', {
                'text': dbFiling_i['pubDate']
            }),
            (edgrPrefix+'xbrlFiling', {
                'nsmap' : edgrNsmap,
                'children':[
                    (edgrPrefix+x, {
                        'text': str(dbFiling_i.get(x)),
                        'nsmap' : edgrNsmap}                  
                            ) for x in filingCols] + files_map
                
            })
        ]

        # seed for item element
        itemEl = etree.Element('item')
        
        # create xml element for each component of the dict above
        for k, v in xml_db_mapping:
            makeEl(k,v, itemEl)

        rssChannel.append(itemEl)
    
    fpath = None
    _saveAs = os.path.abspath(saveAs) if saveAs else saveAs
    if _saveAs: 
        if os.path.isdir(os.path.dirname(_saveAs)):
            filePath, fileExt = os.path.splitext(_saveAs)
            fpath = filePath + '.xml'
            with open(fpath, 'wb') as f:
                f.write(etree.tostring(rssDoc, pretty_print=True))
        else:
            conn.addToLog(_("Couldn't find dir {}").format(os.path.dirname(_saveAs)), messageCode="RssDB.Error", file=conn.conParams.get('database', ''),  level=logging.ERROR)
            return
    else:
        fd = xmlFileFromString(xmlString = etree.tostring(rssDoc), filePrefix='rssDB_search_', tempDir=conn.cntlr.userAppTempDir, deleteF=False)
        fpath = fd.name
    rssItems = []
    if returnRssItems and os.path.isfile(fpath):
        c = CntlrPy(instConfigDir=os.path.dirname(conn.cntlr.userAppDir), useResDir= os.path.dirname(conn.cntlr.imagesDir))
        c.runKwargs(file=fpath)
        modelXbrl = c.modelManager.modelXbrl
        rssItems = modelXbrl.modelDocument.rssItems
        for rssIt in rssItems:
            rssIt.filingId = rssIt.find('filingId').text
    return fpath, rssItems

def runRenderEdgar(mainCntlr, rssItems=None, saveToFolder=None, pluginsDirs=None):
    reportFolder = None
    try:
        reportFolder = renderEdgarReportsFromRssItems(mainCntlr=mainCntlr, rssItems=rssItems, saveToFolder=saveToFolder, pluginsDirs=pluginsDirs)
    except Exception as e:
        mainCntlr.addToLog(_('Error while rendering Edgar Reports\n{}').format(str(e)), messageCode="RssDB.Error", file="runRenderEdgar",  level=logging.ERROR)

    return reportFolder   

def initLocalEdgarViewer(cntlr, lookinFolder=None, edgarDir=None, threaded=True):
    initViewer(cntlr=cntlr, lookinFolders=lookinFolder, edgarDir=edgarDir, threaded=threaded)
