# from db_test import cursor
from db_test import cnxn
import pickle

# members = []
# flat_member_names = {}


class MemberCache:
    def __init__(self, member):
        self.lastName = member.NAME_LAST
        self.firstName = member.NAME_FIRST


def get_flat_member_names():
    cursor = cnxn.cursor()
    query_list = """select nn.NAME_ID, nn.NAME_FIRST, nn.NAME_LAST, nn.TEXT2 as CCA_ID 
    from MPSnapshotProd.dbo.NAME as nn
    left join ReportSupport.dbo.Member_Spans as ms on ms.NAME_ID = nn.NAME_ID
    WHERE ms.ENR_STAT is not null;"""
    result = cursor.execute(query_list)
    members = [row for row in result]
   
    flat_member_names = {" ".join([member.NAME_FIRST.strip() if member.NAME_FIRST else '', member.NAME_LAST.strip() if member.NAME_LAST else '']) for member in members}
    
    # flat_member_objs = {member.CCA_ID: member for member in members}
    cursor.close()
    try:
        flat_member_names.remove(" ")
    except (RuntimeError, KeyError):
        pass

    print('flat fetched')
    return flat_member_names


def write_member():
    flat_member_names = get_flat_member_names()
    with open('myfile.dat', 'wb+') as file:
        pickle.dump(flat_member_names, file)


def read_flat_member():
    with open('myfile.dat', 'rb') as file: 
        flat_member_names = pickle.load(file)
        print(' * Total ' ,len(flat_member_names), 'in caches', list(flat_member_names)[0])
        return flat_member_names

# write_member()