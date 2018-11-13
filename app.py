from flask import Flask, json, request
from flask_restful import Api
from sim_data import sim_members
from flask_cors import CORS
from flask_jwt import JWT

from dateutil import parser as date_parser
import re 

from security import authenticate, identity
from member import Member
from member_flat_name import read_flat_member
from db_test import cnxn

from user import UserRegister

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.secret_key = 'cca'
api = Api(app)
CORS(app)

jwt = JWT(app, authenticate, identity)

members = sim_members
member_objs = [Member(member) for member in sim_members]


filtered_member = members
# reduce_member = list(map(reduce(lambda flat , string: flat + string.replace(" ",''), members)))
flat_member_name = {" ".join([member['firstName'], member['lastName']]) for member in members}
flat_member_obj = {member.firstName + member.lastName: member for member in member_objs}
flat_member_names = read_flat_member()

@app.route('/')
def home():
    return 'Hello, World!'

# GET /member/<string:name>
@app.route('/member/<int:id>')
def get_member(id):
    data_max_len = len(members)
    if data_max_len - 1 < id:
        data = 'invalid id'
        code = 200
    else:
        data = members[id]
        code = 404
    response = Flask.response_class(
        response=json.dumps(data),
        status=code,
        mimetype='application/json')
    return response

# @app.route('/member/list', methods=['POST'])
def get_members():
    request_data = request.get_json()
    # print(request_data,request_data['offset'])
    respond_data = request_data
    # print({ "members": filtered_member[request_data['offset']:request_data['offset'] + request_data['limit']]})
    respond_data.update({
        "count": len(filtered_member),
        "members": filtered_member[request_data['offset'] : request_data['offset'] + request_data['limit']]
    })
  
    response = Flask.response_class(
        response=json.dumps(respond_data),
        status=200,
        mimetype='application/json')
    return response

@app.route('/member/search', methods=['POST'])
def search_members():
    request_data = request.get_json()
    print(request_data)
    respond_data = request_data
    filtered_member = []
    for memebr in members:
        name = ' '.join([memebr['firstName'], memebr['lastName']])
        match_name = re.search(r"{}".format(request_data['name']), name) if request_data['name'] != '' else True
        match_dob = re.search(r"{}".format(request_data['dob']), memebr['dob']) if request_data['dob'] != '' else True
        match_city = re.search(r"{}".format(request_data['city']), memebr['address']) if request_data['city'] != '' else True
        match_county = re.search(r"{}".format(request_data['county']), memebr['address']) if request_data['county'] != '' else True
        # match_city = re.search(r"{}".format(request_data['city']), memebr['address']) if request_data['city'] != '' else True
        # print (match1)
        if match_name and match_dob and match_city and match_county:
            filtered_member.append(memebr)
            print(memebr)
    respond_data.update({
        "count": len(filtered_member),
        "members": filtered_member[request_data['offset'] : request_data['offset'] + request_data['limit']]
    })
  
    response = Flask.response_class(
        response=json.dumps(respond_data),
        status=200,
        mimetype='application/json')
    return response

@app.route('/member/search_test', methods=['POST'])
def search_members_test():
    request_data = request.get_json()
    respond_data = request_data
    offset = request_data.get('offset', 0)
    limit = request_data.get('limit', 50)
    sortColoumn = request_data.get('sortColoumn', "nn.NAME_ID")
    sortDirection = request_data.get('sortDirection', 'ASC')
    print(sortColoumn, sortDirection, offset, limit)

    # #######################################################
    filter_query = memebr_filter_query(request_data)

    respond_data.update({"query": filter_query})
    # print(respond_data)
    response = Flask.response_class(
    response=json.dumps(respond_data),
    status=200,
    mimetype='application/json')
    return response


@app.route('/member/quick_search', methods=['POST'])
def name_search():
    request_data = request.get_json()
    search_str = request_data['search_str']
    matches = list(
        filter(lambda match : match, 
            map(lambda name : {"start": name.lower().find(search_str), "suggestion": name} if (name.lower().find(search_str) >= 0) else None,
             flat_member_name)
             ))

    matches = sorted(matches, key=lambda k: k['start']) 
    print(matches)
    response = Flask.response_class(
    response=json.dumps({"result": matches}),
    status=200,
    mimetype='application/json')
    return response


@app.route('/member/quick_search_test', methods=['POST'])
def name_search_test():
    request_data = request.get_json()
    search_str = request_data['search_str'].lower()
    matches = list(
        filter(lambda match : match, 
            map(lambda name : {"start": name.lower().find(search_str), "suggestion": name} if (name.lower().find(search_str) >= 0) else None, 
            flat_member_names)))

    matches = sorted(matches, key=lambda k: k['start'])[:5]
    print(matches)
    response = Flask.response_class(
    response=json.dumps({"result": matches}),
    status=200,
    mimetype='application/json')
    return response

@app.route('/member/list', methods=['POST'])
def member_list_test():
    cursor = cnxn.cursor()
    request_data = request.get_json()
    respond_data = request_data
    # PAGINATION CONDITION
    offset = request_data.get('offset', 0)
    limit = request_data.get('limit', 50)
    sortColoumn = request_data.get('sortColoumn', "nn.NAME_ID")
    sortDirection = request_data.get('sortDirection', 'ASC')
    # SEARCHING CRITERIA
    filter_query, filter_params = memebr_filter_query(request_data)
    print(sortColoumn, sortDirection, offset, limit)
    print("    ")
    print("    ")
    query_count = """select count(*) 
                    from MPSnapshotProd.dbo.NAME as nn
                    left join MPSnapshotProd.dbo.NAME_ADDRESS as na on na.NAME_ID = nn.NAME_ID and na.ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= na.START_DATE and (GETDATE() <= na.START_DATE or na.END_DATE is null)
                    left join ReportSupport.dbo.Member_Spans as ms on ms.NAME_ID = nn.NAME_ID
                    WHERE ms.ENR_STAT is not null {}""".format(filter_query)
    print('query_count  ######################################################')
    print(query_count)
    print("    ")
    print("    ")
    print('filter_params #####################################################')
    print(filter_params)
    print("    ")
    print("    ")
    count = cursor.execute(query_count, filter_params).fetchval()
    print('count #####################################################')
    print(count)
    print("    ")
    print("    ")

    query_list = """select nn.NAME_ID as NAME_ID, nn.NAME_FIRST as NAME_FIRST,nn.NAME_LAST as NAME_LAST,nn.BIRTH_DATE as BIRTH_DATE,nn.TEXT2 as CCA_ID,
                    ms.product as PRODUCT, ms.START_DATE as P_START_DATE, ms.END_DATE as P_END_DATE, ms.ENR_STAT,
                    na.ADDRESS1, na.ADDRESS2, na.ADDRESS3, na.CITY, na.STATE, na.ZIP, na.COUNTRY, na.COUNTY, na.START_DATE, na.END_DATE
                    from MPSnapshotProd.dbo.NAME as nn
                    left join MPSnapshotProd.dbo.NAME_ADDRESS as na on na.NAME_ID = nn.NAME_ID and na.ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= na.START_DATE and (GETDATE() <= na.START_DATE or na.END_DATE is null)
                    left join ReportSupport.dbo.Member_Spans as ms on ms.NAME_ID = nn.NAME_ID
                    WHERE ms.ENR_STAT is not null {}
                    ORDER BY {} {} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY""".format(filter_query, sortColoumn, sortDirection)
    print('query_list ######################################################')
    print(query_list)
    print("    ")
    print("    ")

    filter_params += (offset, limit)
    print('filter_params ######################################################')
    print(filter_params)
    print("    ")
    print("    ")
    result = cursor.execute(query_list, filter_params)
    print('result ######################################################')
    print(result)
    print("    ")
    print("    ")
    members = list(map(lambda row: {
    'ccaid': row.CCA_ID,
    'lastName': row.NAME_LAST, 
    'firstName': row.NAME_FIRST, 
    'address': "{}{}, {}, {} {}, {}".format(row.ADDRESS1, (' ' + row.ADDRESS2) if row.ADDRESS2 else '', row.CITY, row.STATE, row.ZIP, row.COUNTY), 
    'dob': row.BIRTH_DATE.strftime('%Y-%m-%d') if row.BIRTH_DATE else None,
    'program': row.PRODUCT, 
    'programStart': row.P_START_DATE.strftime('%Y-%m-%d') if row.P_START_DATE else None, 
    'programEnd': row.P_END_DATE.strftime('%Y-%m-%d') if row.P_END_DATE else None} , result))
    cursor.close()
    respond_data.update({"sortColoumn": sortColoumn, "sortDirection": sortDirection, "count": count, "members": members})
    # print(respond_data)
    response = Flask.response_class(
    response=json.dumps(respond_data),
    status=200,
    mimetype='application/json')
    return response

@app.route('/address/filterlocation/list', methods=['GET'])
def address_city_county_list():
    cursor = cnxn.cursor()
    query_list = """select distinct(county) from MPSnapshotProd.dbo.NAME_ADDRESS where county is not null and ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= START_DATE and (GETDATE() <= START_DATE or END_DATE is null)
                    select distinct(city) from MPSnapshotProd.dbo.NAME_ADDRESS where county is not null and ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= START_DATE and (GETDATE() <= START_DATE or END_DATE is null)"""
    result = cursor.execute(query_list)
    respond_data = {'counties': list({row.county.lower() for row in result.fetchall()})}
    result.nextset()
    respond_data.update({'cities': list({row.city.lower() for row in result.fetchall()})})
    cursor.close()
    response = Flask.response_class(
    response=json.dumps(respond_data),
    status=200,
    mimetype='application/json')
    return response


@app.route('/member/list/report', methods=['POST'])
def memebr_list_report():
    request_data = request.get_json()
    ico_ccaids = request_data['ico']
    sco_ccaids = request_data['sco']
    ico_query = "','".join(ico_ccaids) if len(ico_ccaids) > 0 else None
    sco_query = "','".join(sco_ccaids) if len(sco_ccaids) > 0 else None

    ico_query =  """select nn.NAME_ID as NAME_ID, nn.TEXT2 as CCA_ID, nn.NAME_FIRST as NAME_FIRST, nn.NAME_LAST as NAME_LAST,nn.BIRTH_DATE as BIRTH_DATE, nn.SOC_SEC as SSN,
                    ms.product as PRODUCT, ms.START_DATE as P_START_DATE, ms.END_DATE as P_END_DATE, ms.ENR_STAT,
                    rc.rc as RC, rc.RCStart as RCStart, rc.RCEnd as RCEnd,
                    na.ADDRESS1, na.ADDRESS2, na.ADDRESS3, na.CITY, na.STATE, na.ZIP, na.COUNTRY, na.COUNTY, na.START_DATE, na.END_DATE
                    from MPSnapshotProd.dbo.NAME as nn
                    left join MPSnapshotProd.dbo.NAME_ADDRESS as na on na.NAME_ID = nn.NAME_ID and na.ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= na.START_DATE and (GETDATE() <= na.START_DATE or na.END_DATE is null)
                    left join ReportSupport.dbo.Member_Spans as ms on ms.NAME_ID = nn.NAME_ID and GETDATE() >= ms.START_DATE and (GETDATE() <= ms.END_DATE or ms.END_DATE is null)
                   	left join ReportSupport.dbo.vwICORatingCategories_MdsAssist as rc on rc.MPMemberId = nn.NAME_ID and GETDATE() >= rc.RCStart and (GETDATE() <= rc.RCEnd or rc.RCEnd is null) 
                   	where nn.TEXT2 in ('{}')""".format(ico_query) if ico_query else ''

    sco_query = """ select nn.NAME_ID as NAME_ID, nn.TEXT2 as CCA_ID, nn.NAME_FIRST as NAME_FIRST, nn.NAME_LAST as NAME_LAST,nn.BIRTH_DATE as BIRTH_DATE, nn.SOC_SEC as SSN,
                    ms.product as PRODUCT, ms.START_DATE as P_START_DATE, ms.END_DATE as P_END_DATE, ms.ENR_STAT,
                    rc.rc as RC, rc.RCStart as RCStart, rc.RCEnd as RCEnd,
                    na.ADDRESS1, na.ADDRESS2, na.ADDRESS3, na.CITY, na.STATE, na.ZIP, na.COUNTRY, na.COUNTY, na.START_DATE, na.END_DATE
                    from MPSnapshotProd.dbo.NAME as nn
                    left join MPSnapshotProd.dbo.NAME_ADDRESS as na on na.NAME_ID = nn.NAME_ID and na.ADDRESS_TYPE = 'PERMANENT' and GETDATE() >= na.START_DATE and (GETDATE() <= na.START_DATE or na.END_DATE is null)
                    left join ReportSupport.dbo.Member_Spans as ms on ms.NAME_ID = nn.NAME_ID and GETDATE() >= ms.START_DATE and (GETDATE() <= ms.END_DATE or ms.END_DATE is null)
                   	left join ReportSupport.dbo.vwICORatingCategories_MdsAssist as rc on rc.MPMemberId = nn.NAME_ID and GETDATE() >= rc.RCStart and (GETDATE() <= rc.RCEnd or rc.RCEnd is null) 
                   	where nn.TEXT2 in ('{}')""".format(sco_query) if sco_query else ''
                       
    cursor = cnxn.cursor()
    query_list = ico_query + sco_query

    result = cursor.execute(query_list)
    members = jsonify_members(result)

    if ico_query and sco_query:
        result.nextset() 
        members = members + jsonify_members(result)

    print(' query_list ######################################################')
    print(query_list)
    print(' query_list ######################################################')
    print("    ")
    print("    ")
    print(' members ######################################################')
    print(members)
    print(' members ######################################################')
    respond_data = {'members': members}
    # respond_data = {'members': [row for row in result.fetchall()]}
    # result.nextset()
    # respond_data['members'].append(list(row for row in result.fetchall()))
    cursor.close()
    response = Flask.response_class(
    response=json.dumps(respond_data),
    status=200,
    mimetype='application/json')
    return response    

def memebr_filter_query(request_data):
    city = request_data.get("city", None)
    county = request_data.get("county", None)
    disenrolled = request_data.get("disenrolled", None)
    dob = request_data.get("dob", None)
    dob = date_parser.isoparse(dob).date() if dob else None
    firstName = request_data.get("firstName", None)
    lastName = request_data.get("lastName", None)
    program = request_data.get("program", None)
    programEndDate = request_data.get("programEndDate", None)
    programEndDate = date_parser.isoparse(programEndDate).date() if programEndDate else None
    programStartDate = request_data.get("programStartDate", None)
    programStartDate = date_parser.isoparse(programStartDate).date() if programStartDate else None

    print ({'city': city, 'county': county, 'disenrolled': disenrolled, 'dob': dob, 'firstName': firstName, 'lastName':lastName, 'program': program, 'pE': programEndDate, "ps": programStartDate})
    
    filter_query = """"""
    filter_params = ()
    filter_query = filter_query + """AND nn.NAME_FIRST like ? """ if firstName else filter_query
    filter_params =  filter_params + (firstName + '%', ) if firstName else filter_params
    filter_query = filter_query + """AND nn.NAME_LAST like ? """ if lastName else filter_query
    filter_params =  filter_params + (lastName + '%', ) if lastName else filter_params
    filter_query = filter_query + """AND nn.BIRTH_DATE = ? """ if dob else filter_query
    filter_params =  filter_params + (dob, ) if dob else filter_params
    filter_query = filter_query + """AND na.CITY = ? """ if city else filter_query
    filter_params =  filter_params + (city, ) if city else filter_params
    filter_query = filter_query + """AND na.COUNTY = ? """ if county else filter_query
    filter_params =  filter_params + (county, ) if county else filter_params
    filter_query = filter_query + """AND ms.PRODUCT = ? """ if program else filter_query
    filter_params =  filter_params + (program, ) if program else filter_params
    filter_query = filter_query + """AND ms.START_DATE >= ? """ if programStartDate else filter_query
    filter_params =  filter_params + (programStartDate, ) if programStartDate else filter_params
    filter_query = filter_query + """AND ms.END_DATE <= ? """ if programEndDate else filter_query
    filter_params =  filter_params + (programEndDate, ) if programEndDate else filter_params
    filter_query = filter_query if disenrolled else filter_query + """AND  GETDATE() >= ms.START_DATE and (GETDATE() <= ms.START_DATE or ms.END_DATE is null) """
    return filter_query, filter_params

def jsonify_members(result):
    members = list(map(lambda row: {
    'CCA_ID': row.CCA_ID,
    'Last Name': row.NAME_LAST, 
    'First Name': row.NAME_FIRST, 
    'Address': "{}{}, {}, {} {}, {}".format(row.ADDRESS1, (' ' + row.ADDRESS2) if row.ADDRESS2 else '', row.CITY, row.STATE, row.ZIP, row.COUNTY), 
    'Date of Birth': row.BIRTH_DATE.strftime('%Y-%m-%d') if row.BIRTH_DATE else None,
    'SSN': row.SSN,
    'Enrollment Status': row.ENR_STAT,
    'Rating Category': row.RC,
    'rcStart': row.RCStart.strftime('%Y-%m-%d') if row.RCStart else None,
    'rcEnd': row.RCEnd.strftime('%Y-%m-%d') if row.RCEnd else None,
    'Program': row.PRODUCT, 
    'Program Start': row.P_START_DATE.strftime('%Y-%m-%d') if row.P_START_DATE else None, 
    'Program End': row.P_END_DATE.strftime('%Y-%m-%d') if row.P_END_DATE else 'present',
    } , result.fetchall()))
    return members



api.add_resource(UserRegister, '/register')

if __name__ == "__main__":
    app.run(debug=True)

