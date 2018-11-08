from sim_data import sim_members
from member import Member
import re
from functools import reduce

members = sim_members
member_objs = [Member(member) for member in sim_members]

filtered_member = members
# reduce_member = list(map(
#     reduce(lambda flat, value: flat + value.replace(" ",''), [value for attr, value in vars(member_obj).items()]), member_objs))
flat_member = {member['firstName'] + member['lastName'] for member in members}
flat_member_obj = {member.firstName + member.lastName: member for member in member_objs}
print(flat_member,flat_member_obj)

for  value in  vars(member_objs[0]).items():
    print(value)

    
#    / reduce(lambda flat, value: flat + value.replace(" ",''), [value for attr, value in vars(member_objs[1]).items()]))
print(list(map(
    lambda member_obj: reduce(lambda flat, value: flat + value.replace(" ",''), 
    [value for attr, value in vars(member_obj).items()]),
     member_objs)))