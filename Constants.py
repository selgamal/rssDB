'''Constants and functions used by rssDB plugin
'''

import os
from urllib import request
from lxml import html
from collections import OrderedDict
from arelle.DialogRssWatch import rssFeeds


RSSFEEDS = rssFeeds
pathToModule = os.path.dirname(os.path.abspath(__file__))
pathToSQL = os.path.join(pathToModule, 'ddlScripts')
pathToTemplates =  os.path.join(pathToModule, 'templates')
pathToResources = os.path.join(pathToModule, 'resources')


rssTables = ['feedsInfo', 'filingsInfo', 'filesInfo', 'filersInfo', 'rssItems', 'cikTickerMapping', 'lastUpdate', 'formulae', 'formulaeResults']

rssCols = OrderedDict(
    [
        (
            rssTables[0],
            ['feedId', 'feedMonth', 'title', 'link', 'feedLink', 'description',
                'language', 'pubDate', 'lastBuildDate',
                'lastModifiedDate']
        ),
        (
            rssTables[1],
            ['filingId', 'feedId', 'filingLink', 'entryPoint', 'enclosureUrl', 'enclosureSize', 'pubDate', 'companyName',
                'formType', 'inlineXBRL', 'filingDate', 'cikNumber', 'accessionNumber', 'fileNumber',
                'acceptanceDatetime', 'period', 'assignedSic', 'assistantDirector', 'fiscalYearEnd', 'fiscalYearEndMonth',
                'fiscalYearEndDay', 'duplicate']
        ),
        (
            rssTables[2],
            ['fileId', 'filingId', 'feedId', 'accessionNumber', 'sequence', 'file', 'type', 'size',
                'description', 'inlineXBRL', 'url', 'type_tag']
        ),
        (
            rssTables[3],
            ['cikNumber', 'formerNames', 'industry_code', 'industry_description', 'stateOfIncorporation', 'mailingState',
                'mailingCity', 'mailingZip', 'conformedName', 'businessCity', 'businessState', 'businessZip', 'country']
        ),
        (
            rssTables[4],
            ['filingId', 'rssItem']
        ),
        (
            rssTables[5],
            ['tickerSymbol', 'cikNumber']
        ),
        (
            rssTables[6],
            ['id', 'lastUpdate']
        ),
        (
            rssTables[7],
            ['formulaId', 'fileName', 'description', 'formulaLinkbase', 'dateTimeAdded']
        ),
        (
            rssTables[8],
            ['filingId', 'formulaId', 'inlineXBRL', 'formulaOutput', 'assertionsResults', 'dateTimeProcessed', 'processingLog']
        )
    ]
)

rssViews = OrderedDict(
    [
        ('v_duplicate_filings', ['filingId']),
        ('v_filingsSummary', ['feedId'])
    ]
)

DBTypes = ("postgres", "sqlite", "mongodb")

getTablesFuncs = {
    'postgres': lambda x: x.tablesInDB(),
    'sqlite': lambda x: x.tablesInDB(),
    'mongodb': lambda x: x.dbConn.list_collection_names()
}

wait_duration = 1

def _getEdgarStateCodes(getLocation=True):
    """Extracts Edgar state codes from 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm'"""
    url = 'https://www.sec.gov/edgar/searchedgar/edgarstatecodes.htm'
    countries_resp = request.urlopen(url)
    tree = html.parse(countries_resp)
    root = tree.getroot()
    table = root.xpath('.//th[text()="Code"]/ancestor::table[1]')[0]
    trs = table.findall('.//tr')
    titles = []
    subData = ''
    result = []
    for tr in trs:
        children = tr.getchildren()
        if all([x.tag=='th' for x in children]):
            if len(children)>1:
                titles = [x.text_content().strip() for x in children]
            if len(children)==1:
                subData = children[0].text_content().strip()
        elif all([x.tag=='td' for x in children]):
            data = [x.text.strip() for x in children]
            result.append({**dict(zip(titles, data)), 'header':subData})

    lookupDict = {
        'States':  lambda x: ('US', x['State or Country Name']),
        'Canadian Provinces': lambda x: ('CANADA', x['State or Country Name']),
        'Other Countries': lambda x: (x.get('State or Country Name'), None)
    }

    codes = OrderedDict()
    for x in result:
        codes[x['Code']] = lookupDict[x['header']](x)
    
    # add location info
    if getLocation:
        try:
            from geopy.geocoders import Nominatim
            geolocator = Nominatim(user_agent="testing")
            fixes = {
            'WA': ('US', 'WASHINGTON STATE'), # WASHINGTON
            'Z4': ('CANADA', 'CANADA'), #'CANADA (Federal Level)
            'Y3': ('DEMOCRATIC REPUBLIC OF THE CONGO', None), # CONGO, THE DEMOCRATIC REPUBLIC OF THE
            'X4': ('VATICAN CITY STATE', None),
            '1S': ('MOLDOVA', None), #MOLDOVA, REPUBLIC OF
            '1K': ('FEDERATED STATES OF MICRONESIA', None), #  MICRONESIA, FEDERATED STATES OF
            'M4': ('NORTH KOREA', None),
            'M5': ('SOUTH KOREA', None),
            '1U': ('MACEDONIA', None),
            '1X': ('PALESTINE', None),
            'K9': ('ISLAMIC REPUBLIC OF IRAN', None),
            'F5': ('TAIWAN', None),
            'W0': ('UNITED REPUBLIC OF TANZANIA', None),
            '2J': ('US', 'DISTRICT OF COLUMBIA'), #'UNITED STATES MINOR OUTLYING ISLANDS' reported as DC, US
            'D8': ('BRITISH VIRGIN ISLANDS', None),
            'VI': ('U.S. VIRGIN ISLANDS', None),
            'XX': ('US', 'DISTRICT OF COLUMBIA') #'UNKNOWN' reported as DC, US
            }

            for d in codes:
                _location = fixes[d] if d in fixes else codes[d]
                if _location[0]=='US':
                    loc = geolocator.geocode(_location[1] + ', ' + _location[0], language='en')
                elif _location[0]=='CANADA':
                    loc = geolocator.geocode(_location[1], language='en')
                else:
                    loc = geolocator.geocode(_location[0], language='en')
                print(loc, ' -- ', ' '.join(filter(None, _location)), ' -- ', ' '.join(filter(None, codes[d])))
                codes[d] = codes[d] + (loc.latitude, loc.longitude) + (', '.join(filter(None, (*reversed(_location),))) if d in fixes else None,) # location fix if any
        except:
            pass
    
    return codes


def _getSP100():
    url = 'https://en.wikipedia.org/wiki/S%26P_100'
    sp100_resp = request.urlopen(url)
    tree = html.parse(sp100_resp)
    root = tree.getroot()
    trs = root.xpath('.//table[@id="constituents"]//tbody/tr')
    sp100Tkrs = []
    for tr in trs:
        sp100Tkrs.append(tuple(d.text.replace('\n', '') if d.text else d.xpath('.//a/text()')[0] for d in tr.findall('td')))

    tks_site = [x[0].lower().replace('.', '-') for x in sp100Tkrs if x]
    tks_site.sort()
    return list(set(tks_site))

# States/Countries codes data => country name, state name (US), latitude, longitude, location fix name (name used to get coordinates)
stateCodes  = OrderedDict([
             ('AL', ('US', 'ALABAMA', 33.2588817, -86.8295337, None)),
             ('AK', ('US', 'ALASKA', 64.4459613, -149.680909, None)),
             ('AZ', ('US', 'ARIZONA', 34.395342, -111.7632755, None)),
             ('AR', ('US', 'ARKANSAS', 35.2048883, -92.4479108, None)),
             ('CA', ('US', 'CALIFORNIA', 36.7014631, -118.7559974, None)),
             ('CO', ('US', 'COLORADO', 38.7251776, -105.6077167, None)),
             ('CT', ('US', 'CONNECTICUT', 41.6500201, -72.7342163, None)),
             ('DE', ('US', 'DELAWARE', 38.6920451, -75.4013315, None)),
             ('DC', ('US', 'DISTRICT OF COLUMBIA', 38.893661249999994, -76.98788325388196, None)),
             ('FL', ('US', 'FLORIDA', 27.7567667, -81.4639835, None)),
             ('GA', ('US', 'GEORGIA', 32.3293809, -83.1137366, None)),
             ('HI', ('US', 'HAWAII', 19.58726775, -155.42688965312746, None)),
             ('ID', ('US', 'IDAHO', 43.6447642, -114.0154071, None)),
             ('IL', ('US', 'ILLINOIS', 40.0796606, -89.4337288, None)),
             ('IN', ('US', 'INDIANA', 40.3270127, -86.1746933, None)),
             ('IA', ('US', 'IOWA', 41.9216734, -93.3122705, None)),
             ('KS', ('US', 'KANSAS', 38.27312, -98.5821872, None)),
             ('KY', ('US', 'KENTUCKY', 37.5726028, -85.1551411, None)),
             ('LA', ('US', 'LOUISIANA', 30.8703881, -92.007126, None)),
             ('ME', ('US', 'MAINE', 45.709097, -68.8590201, None)),
             ('MD', ('US', 'MARYLAND', 39.5162234, -76.9382069, None)),
             ('MA', ('US', 'MASSACHUSETTS', 42.3788774, -72.032366, None)),
             ('MI', ('US', 'MICHIGAN', 43.6211955, -84.6824346, None)),
             ('MN', ('US', 'MINNESOTA', 45.9896587, -94.6113288, None)),
             ('MS', ('US', 'MISSISSIPPI', 32.9715645, -89.7348497, None)),
             ('MO', ('US', 'MISSOURI', 38.7604815, -92.5617875, None)),
             ('MT', ('US', 'MONTANA', 47.3752671, -109.6387579, None)),
             ('NE', ('US', 'NEBRASKA', 41.7370229, -99.5873816, None)),
             ('NV', ('US', 'NEVADA', 39.5158825, -116.8537227, None)),
             ('NH', ('US', 'NEW HAMPSHIRE', 43.4849133, -71.6553992, None)),
             ('NJ', ('US', 'NEW JERSEY', 40.0757384, -74.4041622, None)),
             ('NM', ('US', 'NEW MEXICO', 34.5708167, -105.993007, None)),
             ('NY', ('US', 'NEW YORK', 40.7127281, -74.0060152, None)),
             ('NC', ('US', 'NORTH CAROLINA', 35.6729639, -79.0392919, None)),
             ('ND', ('US', 'NORTH DAKOTA', 47.6201461, -100.540737, None)),
             ('OH', ('US', 'OHIO', 40.2253569, -82.6881395, None)),
             ('OK', ('US', 'OKLAHOMA', 34.9550817, -97.2684063, None)),
             ('OR', ('US', 'OREGON', 43.9792797, -120.737257, None)),
             ('PA', ('US', 'PENNSYLVANIA', 40.9699889, -77.7278831, None)),
             ('RI', ('US', 'RHODE ISLAND', 41.7962409, -71.5992372, None)),
             ('SC', ('US', 'SOUTH CAROLINA', 33.6874388, -80.4363743, None)),
             ('SD', ('US', 'SOUTH DAKOTA', 44.6471761, -100.348761, None)),
             ('TN', ('US', 'TENNESSEE', 35.7730076, -86.2820081, None)),
             ('TX', ('US', 'TEXAS', 31.8160381, -99.5120986, None)),
             ('X1', ('US', 'UNITED STATES', 39.7837304, -100.4458825, None)),
             ('UT', ('US', 'UTAH', 39.4225192, -111.7143584, None)),
             ('VT', ('US', 'VERMONT', 44.5990718, -72.5002608, None)),
             ('VA', ('US', 'VIRGINIA', 37.1232245, -78.4927721, None)),
             ('WA', ('US', 'WASHINGTON', 47.2868352, -120.2126139, 'WASHINGTON STATE, US')),
             ('WV', ('US', 'WEST VIRGINIA', 38.4758406, -80.8408415, None)),
             ('WI', ('US', 'WISCONSIN', 44.4308975, -89.6884637, None)),
             ('WY', ('US', 'WYOMING', 43.1700264, -107.5685348, None)),
             ('A0', ('CANADA', 'ALBERTA, CANADA', 55.001251, -115.002136, None)), 
             ('A1', ('CANADA', 'BRITISH COLUMBIA, CANADA', 55.001251, -125.002441, None)),
             ('A2', ('CANADA', 'MANITOBA, CANADA', 55.001251, -97.001038, None)),
             ('A3', ('CANADA', 'NEW BRUNSWICK, CANADA', 46.500283, -66.750183, None)),
             ('A4', ('CANADA', 'NEWFOUNDLAND, CANADA', 49.12120935, -56.69629621274099, None)),
             ('A5', ('CANADA', 'NOVA SCOTIA, CANADA', 45.1960403, -63.1653789, None)),
             ('A6', ('CANADA', 'ONTARIO, CANADA', 50.000678, -86.000977, None)),
             ('A7', ('CANADA', 'PRINCE EDWARD ISLAND, CANADA', 46.503545349999996, -63.595517139914485, None)),
             ('A8', ('CANADA', 'QUEBEC, CANADA', 52.4760892, -71.8258668, None)),
             ('A9', ('CANADA', 'SASKATCHEWAN, CANADA', 55.5321257, -106.1412243, None)),
             ('B0', ('CANADA', 'YUKON, CANADA', 63.000147, -136.002502, None)),
             ('Z4', ('CANADA (Federal Level)', None, 61.0666922, -107.9917071, 'CANADA, CANADA')),
             ('B2', ('AFGHANISTAN', None, 33.7680065, 66.2385139, None)),
             ('Y6', ('ALAND ISLANDS', None, 60.1603621, 20.08317860965865, None)),
             ('B3', ('ALBANIA', None, 41.000028, 19.9999619, None)),
             ('B4', ('ALGERIA', None, 28.0000272, 2.9999825, None)),
             ('B5', ('AMERICAN SAMOA', None, -14.289304, -170.692511, None)),
             ('B6', ('ANDORRA', None, 42.5407167, 1.5732033, None)),
             ('B7', ('ANGOLA', None, -11.8775768, 17.5691241, None)),
             ('1A', ('ANGUILLA', None, 18.1954947, -63.0750234, None)),
             ('B8', ('ANTARCTICA', None, -79.4063075, 0.3149312, None)),
             ('B9', ('ANTIGUA AND BARBUDA', None, 17.2234721, -61.9554608, None)),
             ('C1', ('ARGENTINA', None, -34.9964963, -64.9672817, None)),
             ('1B', ('ARMENIA', None, 40.7696272, 44.6736646, None)),
             ('1C', ('ARUBA', None, 12.5013629, -69.9618475, None)),
             ('C3', ('AUSTRALIA', None, -24.7761086, 134.755, None)),
             ('C4', ('AUSTRIA', None, 47.2000338, 13.199959, None)),
             ('1D', ('AZERBAIJAN', None, 40.3936294, 47.7872508, None)),
             ('C5', ('BAHAMAS', None, 24.7736546, -78.0000547, None)),
             ('C6', ('BAHRAIN', None, 26.1551249, 50.5344606, None)),
             ('C7', ('BANGLADESH', None, 24.4768783, 90.2932426, None)),
             ('C8', ('BARBADOS', None, 13.1500331, -59.5250305, None)),
             ('1F', ('BELARUS', None, 53.4250605, 27.6971358, None)),
             ('C9', ('BELGIUM', None, 50.6402809, 4.6667145, None)),
             ('D1', ('BELIZE', None, 16.8259793, -88.7600927, None)),
             ('G6', ('BENIN', None, 9.5293472, 2.2584408, None)),
             ('D0', ('BERMUDA', None, 32.3018217, -64.7603583, None)),
             ('D2', ('BHUTAN', None, 27.549511, 90.5119273, None)),
             ('D3', ('BOLIVIA', None, -17.0568696, -64.9912286, None)),
             ('1E', ('BOSNIA AND HERZEGOVINA', None, 44.3053476, 17.5961467, None)),
             ('B1', ('BOTSWANA', None, -23.1681782, 24.5928742, None)),
             ('D4', ('BOUVET ISLAND', None, -54.4201305, 3.3599732952297483, None)),
             ('D5', ('BRAZIL', None, -10.3333333, -53.2, None)),
             ('D6', ('BRITISH INDIAN OCEAN TERRITORY', None, -6.4157192, 72.1173961, None)),
             ('D9', ('BRUNEI DARUSSALAM', None, 4.4137155, 114.5653908, None)),
             ('E0', ('BULGARIA', None, 42.6073975, 25.4856617, None)),
             ('X2', ('BURKINA FASO', None, 12.0753083, -1.6880314, None)),
             ('E2', ('BURUNDI', None, -3.3634357, 29.8870575, None)),
             ('E3', ('CAMBODIA', None, 13.5066394, 104.869423, None)),
             ('E4', ('CAMEROON', None, 4.6125522, 13.1535811, None)),
             ('E8', ('CAPE VERDE', None, 16.0000552, -24.0083947, None)),
             ('E9', ('CAYMAN ISLANDS', None, 19.5417212, -80.5667132, None)),
             ('F0', ('CENTRAL AFRICAN REPUBLIC', None, 7.0323598, 19.9981227, None)),
             ('F2', ('CHAD', None, 15.6134137, 19.0156172, None)),
             ('F3', ('CHILE', None, -31.7613365, -71.3187697, None)),
             ('F4', ('CHINA', None, 35.000074, 104.999927, None)),
             ('F6', ('CHRISTMAS ISLAND', None, -10.49124145, 105.6173514897963, None)),
             ('F7', ('COCOS (KEELING) ISLANDS', None, -12.0728315, 96.8409375, None)),
             ('F8', ('COLOMBIA', None, 2.8894434, -73.783892, None)),
             ('F9', ('COMOROS', None, -12.2045176, 44.2832964, None)),
             ('G0', ('CONGO', None, -0.7264327, 15.6419155, None)),
             ('Y3', ('CONGO, THE DEMOCRATIC REPUBLIC OF THE', None, -2.9814344, 23.8222636, 'DEMOCRATIC REPUBLIC OF THE CONGO')),
             ('G1', ('COOK ISLANDS', None, -16.0492781, -160.3554851, None)),
             ('G2', ('COSTA RICA', None, 10.2735633, -84.0739102, None)),
             ('L7', ("COTE D'IVOIRE", None, 7.9897371, -5.5679458, None)),
             ('1M', ('CROATIA', None, 45.5643442, 17.0118954, None)),
             ('G3', ('CUBA', None, 23.0131338, -80.8328748, None)),
             ('G4', ('CYPRUS', None, 34.9823018, 33.1451285, None)),
             ('2N', ('CZECH REPUBLIC', None, 49.8167003, 15.4749544, None)),
             ('G7', ('DENMARK', None, 55.670249, 10.3333283, None)),
             ('1G', ('DJIBOUTI', None, 11.8145966, 42.8453061, None)),
             ('G9', ('DOMINICA', None, 19.0974031, -70.3028026, None)),
             ('G8', ('DOMINICAN REPUBLIC', None, 19.0974031, -70.3028026, None)),
             ('H1', ('ECUADOR', None, -1.3397668, -79.3666965, None)),
             ('H2', ('EGYPT', None, 26.2540493, 29.2675469, None)),
             ('H3', ('EL SALVADOR', None, 13.8000382, -88.9140683, None)),
             ('H4', ('EQUATORIAL GUINEA', None, 1.613172, 10.5170357, None)),
             ('1J', ('ERITREA', None, 15.9500319, 37.9999668, None)),
             ('1H', ('ESTONIA', None, 58.7523778, 25.3319078, None)),
             ('H5', ('ETHIOPIA', None, 10.2116702, 38.6521203, None)),
             ('H7', ('FALKLAND ISLANDS (MALVINAS)', None, -51.9666424, -59.5500387, None)),
             ('H6', ('FAROE ISLANDS', None, 62.0448724, -7.0322972, None)),
             ('H8', ('FIJI', None, -18.1239696, 179.0122737, None)),
             ('H9', ('FINLAND', None, 63.2467777, 25.9209164, None)),
             ('I0', ('FRANCE', None, 46.603354, 1.8883335, None)),
             ('I3', ('FRENCH GUIANA', None, 4.0039882, -52.999998, None)),
             ('I4', ('FRENCH POLYNESIA', None, -16.03442485, -146.0490931059517, None)),
             ('2C', ('FRENCH SOUTHERN TERRITORIES', None, -49.237441950000004, 69.62275903679347, None)),
             ('I5', ('GABON', None, -0.8999695, 11.6899699, None)),
             ('I6', ('GAMBIA', None, 13.470062, -15.4900464, None)),
             ('2Q', ('GEORGIA', None, 41.6809707, 44.0287382, None)),
             ('2M', ('GERMANY', None, 51.0834196, 10.4234469, None)),
             ('J0', ('GHANA', None, 8.0300284, -1.0800271, None)),
             ('J1', ('GIBRALTAR', None, 36.140807, -5.3541295, None)),
             ('J3', ('GREECE', None, 38.9953683, 21.9877132, None)),
             ('J4', ('GREENLAND', None, 77.6192349, -42.8125967, None)),
             ('J5', ('GRENADA', None, 12.1360374, -61.6904045, None)),
             ('J6', ('GUADELOUPE', None, 16.2490067, -61.5650444, None)),
             ('GU', ('GUAM', None, 13.450125700000001, 144.75755102972062, None)),
             ('J8', ('GUATEMALA', None, 15.6356088, -89.8988087, None)),
             ('Y7', ('GUERNSEY', None, 49.579520200000005, -2.5290434448309886, None)),
             ('J9', ('GUINEA', None, 10.7226226, -10.7083587, None)),
             ('S0', ('GUINEA-BISSAU', None, 12.100035, -14.9000214, None)),
             ('K0', ('GUYANA', None, 4.8417097, -58.6416891, None)),
             ('K1', ('HAITI', None, 19.1399952, -72.3570972, None)),
             ('K4', ('HEARD ISLAND AND MCDONALD ISLANDS', None, -53.0166353, 72.955751, None)),
             ('X4', ('HOLY SEE (VATICAN CITY STATE)', None, 41.9034912, 12.4528349, 'VATICAN CITY STATE')),
             ('K2', ('HONDURAS', None, 15.2572432, -86.0755145, None)),
             ('K3', ('HONG KONG', None, 22.2793278, 114.1628131, None)),
             ('K5', ('HUNGARY', None, 47.1817585, 19.5060937, None)),
             ('K6', ('ICELAND', None, 64.9841821, -18.1059013, None)),
             ('K7', ('INDIA', None, 22.3511148, 78.6677428, None)),
             ('K8', ('INDONESIA', None, -2.4833826, 117.8902853, None)),
             ('K9', ('IRAN, ISLAMIC REPUBLIC OF', None, 32.6475314, 54.5643516, 'ISLAMIC REPUBLIC OF IRAN')),
             ('L0', ('IRAQ', None, 33.0955793, 44.1749775, None)),
             ('L2', ('IRELAND', None, 52.865196, -7.9794599, None)),
             ('Y8', ('ISLE OF MAN', None, 54.2358167, -4.514598745698255, None)),
             ('L3', ('ISRAEL', None, 31.5313113, 34.8667654, None)),
             ('L6', ('ITALY', None, 42.6384261, 12.674297, None)),
             ('L8', ('JAMAICA', None, 18.1152958, -77.1598454610168, None)),
             ('M0', ('JAPAN', None, 36.5748441, 139.2394179, None)),
             ('Y9', ('JERSEY', None, 49.21230655, -2.1255999596428845, None)),
             ('M2', ('JORDAN', None, 31.1667049, 36.941628, None)),
             ('1P', ('KAZAKSTAN', None, 47.2286086, 65.2093197, None)),
             ('M3', ('KENYA', None, 1.4419683, 38.4313975, None)),
             ('J2', ('KIRIBATI', None, 0.306, 173.664834025, None)),
             ('M4', ("KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF", None, 40.3736611, 127.0870417, 'NORTH KOREA')),
             ('M5', ('KOREA, REPUBLIC OF', None, 36.638392, 127.6961188, 'SOUTH KOREA')),
             ('M6', ('KUWAIT', None, 29.2733964, 47.4979476, None)),
             ('1N', ('KYRGYZSTAN', None, 41.5089324, 74.724091, None)),
             ('M7', ("LAO PEOPLE'S DEMOCRATIC REPUBLIC", None, 20.0171109, 103.378253, None)),
             ('1R', ('LATVIA', None, 56.8406494, 24.7537645, None)),
             ('M8', ('LEBANON', None, 33.8750629, 35.843409, None)),
             ('M9', ('LESOTHO', None, -29.6039267, 28.3350193, None)),
             ('N0', ('LIBERIA', None, 5.7499721, -9.3658524, None)),
             ('N1', ('LIBYAN ARAB JAMAHIRIYA', None, 26.8234472, 18.1236723, None)),
             ('N2', ('LIECHTENSTEIN', None, 47.1416307, 9.5531527, None)),
             ('1Q', ('LITHUANIA', None, 55.3500003, 23.7499997, None)),
             ('N4', ('LUXEMBOURG', None, 49.8158683, 6.1296751, None)),
             ('N5', ('MACAU', None, 22.1757605, 113.5514142, None)),
             ('1U', ('MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF', None, 41.6171214, 21.7168387, 'MACEDONIA')),
             ('N6', ('MADAGASCAR', None, -18.9249604, 46.4416422, None)),
             ('N7', ('MALAWI', None, -13.2687204, 33.9301963, None)),
             ('N8', ('MALAYSIA', None, 4.5693754, 102.2656823, None)),
             ('N9', ('MALDIVES', None, 4.7064352, 73.3287853, None)),
             ('O0', ('MALI', None, 16.3700359, -2.2900239, None)),
             ('O1', ('MALTA', None, 35.8885993, 14.4476911, None)),
             ('1T', ('MARSHALL ISLANDS', None, 6.9518742, 170.9985095, None)),
             ('O2', ('MARTINIQUE', None, 14.6113732, -60.9620777, None)),
             ('O3', ('MAURITANIA', None, 20.2540382, -9.2399263, None)),
             ('O4', ('MAURITIUS', None, -20.2759451, 57.5703566, None)),
             ('2P', ('MAYOTTE', None, -12.823048, 45.1520755, None)),
             ('O5', ('MEXICO', None, 19.4326296, -99.1331785, None)),
             ('1K', ('MICRONESIA, FEDERATED STATES OF', None, 8.6065, 152.00846930625, 'FEDERATED STATES OF MICRONESIA')),
             ('1S', ('MOLDOVA, REPUBLIC OF', None, 47.2879608, 28.5670941, 'MOLDOVA')),
             ('O9', ('MONACO', None, 43.7323492, 7.4276832, None)),
             ('P0', ('MONGOLIA', None, 46.8250388, 103.8499736, None)),
             ('Z5', ('MONTENEGRO', None, 42.9868853, 19.5180992, None)),
             ('P1', ('MONTSERRAT', None, 16.7417041, -62.1916844, None)),
             ('P2', ('MOROCCO', None, 31.1728205, -7.3362482, None)),
             ('P3', ('MOZAMBIQUE', None, -19.302233, 34.9144977, None)),
             ('E1', ('MYANMAR', None, 17.1750495, 95.9999652, None)),
             ('T6', ('NAMIBIA', None, -23.2335499, 17.3231107, None)),
             ('P5', ('NAURU', None, -0.5252306, 166.9324426, None)),
             ('P6', ('NEPAL', None, 28.1083929, 84.0917139, None)),
             ('P7', ('NETHERLANDS', None, 52.5001698, 5.7480821, None)),
             ('P8', ('NETHERLANDS ANTILLES', None, 12.1546009, -68.94047234929069, None)),
             ('1W', ('NEW CALEDONIA', None, -20.454288599999998, 164.55660583077983, None)),
             ('Q2', ('NEW ZEALAND', None, -41.5000831, 172.8344077, None)),
             ('Q3', ('NICARAGUA', None, 12.6090157, -85.2936911, None)),
             ('Q4', ('NIGER', None, 17.7356214, 9.3238432, None)),
             ('Q5', ('NIGERIA', None, 9.6000359, 7.9999721, None)),
             ('Q6', ('NIUE', None, -19.0536414, -169.8613411, None)),
             ('Q7', ('NORFOLK ISLAND', None, -29.0289575, 167.9587289126371, None)),
             ('1V', ('NORTHERN MARIANA ISLANDS', None, 14.149020499999999, 145.21345248318923, None)),
             ('Q8', ('NORWAY', None, 64.5731537, 11.52803643954819, None)),
             ('P4', ('OMAN', None, 21.0000287, 57.0036901, None)),
             ('R0', ('PAKISTAN', None, 30.3308401, 71.247499, None)),
             ('1Y', ('PALAU', None, 6.097367, 133.313631, None)),
             ('1X', ('PALESTINIAN TERRITORY, OCCUPIED', None, 31.94696655, 35.27386547291496, 'PALESTINE')),
             ('R1', ('PANAMA', None, 8.559559, -81.1308434, None)),
             ('R2', ('PAPUA NEW GUINEA', None, -5.6816069, 144.2489081, None)),
             ('R4', ('PARAGUAY', None, -23.3165935, -58.1693445, None)),
             ('R5', ('PERU', None, -6.8699697, -75.0458515, None)),
             ('R6', ('PHILIPPINES', None, 12.7503486, 122.7312101, None)),
             ('R8', ('PITCAIRN', None, -25.0657719, -130.1017823, None)),
             ('R9', ('POLAND', None, 52.215933, 19.134422, None)),
             ('S1', ('PORTUGAL', None, 40.0332629, -7.8896263, None)),
             ('PR', ('PUERTO RICO', None, 18.2214149, -66.41328179513847, None)),
             ('S3', ('QATAR', None, 25.3336984, 51.2295295, None)),
             ('S4', ('REUNION', None, -21.1309332, 55.5265771, None)),
             ('S5', ('ROMANIA', None, 45.9852129, 24.6859225, None)),
             ('1Z', ('RUSSIAN FEDERATION', None, 64.6863136, 97.7453061, None)),
             ('S6', ('RWANDA', None, -1.9646631, 30.0644358, None)),
             ('Z0', ('SAINT BARTHELEMY', None, 17.9036287, -62.811568843006896, None)),
             ('U8', ('SAINT HELENA', None, -15.9656162, -5.702147693859718, None)),
             ('U7', ('SAINT KITTS AND NEVIS', None, 17.250512, -62.6725973, None)),
             ('U9', ('SAINT LUCIA', None, 13.8250489, -60.975036, None)),
             ('Z1', ('SAINT MARTIN', None, 48.5683066, 6.7539988, None)),
             ('V0', ('SAINT PIERRE AND MIQUELON', None, 46.783246899999995, -56.195158907484085, None)),
             ('V1', ('SAINT VINCENT AND THE GRENADINES', None, 12.90447, -61.2765569, None)),
             ('Y0', ('SAMOA', None, -13.7693895, -172.1200508, None)),
             ('S8', ('SAN MARINO', None, 43.9458623, 12.458306, None)),
             ('S9', ('SAO TOME AND PRINCIPE', None, 0.8875498, 6.9648718, None)),
             ('T0', ('SAUDI ARABIA', None, 25.6242618, 42.3528328, None)),
             ('T1', ('SENEGAL', None, 14.4750607, -14.4529612, None)),
             ('Z2', ('SERBIA', None, 44.024322850000004, 21.07657433209902, None)),
             ('T2', ('SEYCHELLES', None, -4.6574977, 55.4540146, None)),
             ('T8', ('SIERRA LEONE', None, 8.6400349, -11.8400269, None)),
             ('U0', ('SINGAPORE', None, 1.357107, 103.8194992, None)),
             ('2B', ('SLOVAKIA', None, 48.7411522, 19.4528646, None)),
             ('2A', ('SLOVENIA', None, 45.8133113, 14.4808369, None)),
             ('D7', ('SOLOMON ISLANDS', None, -9.7354344, 162.8288542, None)),
             ('U1', ('SOMALIA', None, 8.3676771, 49.083416, None)),
             ('T3', ('SOUTH AFRICA', None, -28.8166236, 24.991639, None)),
             ('1L', ('SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS', None, -54.8432857, -35.8090698, None)),
             ('U3', ('SPAIN', None, 39.3262345, -4.8380649, None)),
             ('F1', ('SRI LANKA', None, 7.5554942, 80.7137847, None)),
             ('V2', ('SUDAN', None, 14.5844444, 29.4917691, None)),
             ('V3', ('SURINAME', None, 4.1413025, -56.0771187, None)),
             ('L9', ('SVALBARD AND JAN MAYEN', None, 78.51240255, 16.60558240163109, None)),
             ('V6', ('SWAZILAND', None, -26.5624806, 31.3991317, None)),
             ('V7', ('SWEDEN', None, 59.6749712, 14.5208584, None)),
             ('V8', ('SWITZERLAND', None, 46.7985624, 8.2319736, None)),
             ('V9', ('SYRIAN ARAB REPUBLIC', None, 34.6401861, 39.0494106, None)),
             ('F5', ('TAIWAN, PROVINCE OF CHINA', None, 23.9739374, 120.9820179, 'TAIWAN')),
             ('2D', ('TAJIKISTAN', None, 38.6281733, 70.8156541, None)),
             ('W0', ('TANZANIA, UNITED REPUBLIC OF', None, -6.5247123, 35.7878438, 'UNITED REPUBLIC OF TANZANIA')),
             ('W1', ('THAILAND', None, 14.8971921, 100.83273, None)),
             ('Z3', ('TIMOR-LESTE', None, -8.5151979, 125.8375756, None)),
             ('W2', ('TOGO', None, 8.7800265, 1.0199765, None)),
             ('W3', ('TOKELAU', None, -9.1676396, -171.8196878, None)),
             ('W4', ('TONGA', None, -19.9160819, -175.2026424, None)),
             ('W5', ('TRINIDAD AND TOBAGO', None, 10.8677845, -60.9821067, None)),
             ('W6', ('TUNISIA', None, 33.8439408, 9.400138, None)),
             ('W8', ('TURKEY', None, 38.9597594, 34.9249653, None)),
             ('2E', ('TURKMENISTAN', None, 39.3763807, 59.3924609, None)),
             ('W7', ('TURKS AND CAICOS ISLANDS', None, 21.7214683, -71.6201783, None)),
             ('2G', ('TUVALU', None, -7.768959, 178.1167698, None)),
             ('W9', ('UGANDA', None, 1.5333554, 32.2166578, None)),
             ('2H', ('UKRAINE', None, 49.4871968, 31.2718321, None)),
             ('C0', ('UNITED ARAB EMIRATES', None, 24.0002488, 53.9994829, None)),
             ('X0', ('UNITED KINGDOM', None, 54.7023545, -3.2765753, None)),
             ('2J', ('UNITED STATES MINOR OUTLYING ISLANDS', None, 38.893661249999994, -76.98788325388196, 'DISTRICT OF COLUMBIA, US')),
             ('X3', ('URUGUAY', None, -32.8755548, -56.0201525, None)),
             ('2K', ('UZBEKISTAN', None, 41.32373, 63.9528098, None)),
             ('2L', ('VANUATU', None, -16.5255069, 168.1069154, None)),
             ('X5', ('VENEZUELA', None, 8.0018709, -66.1109318, None)),
             ('Q1', ('VIET NAM', None, 13.2904027, 108.4265113, None)),
             ('D8', ('VIRGIN ISLANDS, BRITISH', None, 18.4024395, -64.5661642, 'BRITISH VIRGIN ISLANDS')),
             ('VI', ('VIRGIN ISLANDS, U.S.', None, 17.789187, -64.7080574, 'U.S. VIRGIN ISLANDS')),
             ('X8', ('WALLIS AND FUTUNA', None, -14.30190495, -178.08997342208266, None)),
             ('U5', ('WESTERN SAHARA', None, 24.1797324, -13.7667848, None)),
             ('T7', ('YEMEN', None, 16.3471243, 47.8915271, None)),
             ('Y4', ('ZAMBIA', None, -14.5186239, 27.5599164, None)),
             ('Y5', ('ZIMBABWE', None, -18.4554963, 29.7468414, None)),
             ('XX', ('UNKNOWN', None, 38.893661249999994, -76.98788325388196, 'DISTRICT OF COLUMBIA, US'))])

# S&P 100 tickers for testing (October 2020 using _getSP100)
sp100 = ['oxy', 'googl', 'wmt', 'abt', 'hon', 'wfc', 'gs', 'dow', 'all', 'amt', 'nvda', 'nflx', 'wba', 'abbv', 'mdt', 'bmy', 'ba', 'cof', 'emr', 'dd', 'unh', 'amzn', 'vz', 'cvs', 'ibm', 'adbe', 'mo', 'bk', 'nee', 'biib', 'sbux', 'gild', 'exc', 'jnj', 'msft', 'hd', 'spg', 'met', 'mcd', 'chtr', 'acn', 'mdlz', 'intc', 'pep', 'ge', 'pfe', 'crm', 'pypl', 'xom',
         'aapl', 'bac', 'csco', 'ms', 'ups', 'blk', 'mmm', 'fb', 'pm', 'brk-b', 'duk', 'ma', 'orcl', 'gm', 'f', 'axp', 'cmcsa', 'low', 'txn', 'amgn', 'slb', 't', 'jpm', 'cop', 'pg', 'ko', 'so', 'khc', 'mrk', 'tmo', 'aig', 'fdx', 'usb', 'bkng', 'c', 'tgt', 'v', 'lmt', 'cl', 'rtx', 'cvx', 'goog', 'dhr', 'qcom', 'lly', 'cat', 'unp', 'gd', 'cost', 'kmi', 'nke', 'dis']
