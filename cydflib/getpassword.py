import os
def get_password():
    print(os.listdir('.'))
    with open("../password.txt", "r") as passwordfile:
        return passwordfile.readlines()[0].rstrip("\n")
