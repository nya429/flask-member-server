class Member:
    def __init__(self, member):
        self.lastName = member['lastName']
        self.firstName = member['firstName']
        self.address = member['address']
        self.dob = member['dob']
        self.mdsStatus = member['mdsStatus']
        self.contactStatus = member['contactStatus']
        self.program = member['program']
