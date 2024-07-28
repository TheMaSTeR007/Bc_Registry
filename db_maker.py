# Creating pincodes table if not exists
pincodes_create_query = f'''CREATE TABLE IF NOT EXISTS pincodes (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            pincode INT,
                            pincode_status VARCHAR(255) DEFAULT 'Pending'
                            );'''

bc_registry_create_query = f'''CREATE TABLE IF NOT EXISTS bc_registry (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            BC_Name VARCHAR(255),
                            Mobile_No BIGINT,
                            Pincode INT,
                            Bank_Name VARCHAR(255)
                            );'''
new_bc_registry_create_query = f'''CREATE TABLE IF NOT EXISTS new_bc_registry (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            BC_Name VARCHAR(255),
                            Mobile_No BIGINT,
                            Pincode INT,
                            Bank_Name VARCHAR(255)
                            );'''

#
# import pymysql
# from pymysql.constants import CLIENT
#
# # # INSERTING ALL PINCODES INTO DB TABLE
# client = pymysql.connect(
#     host='localhost',
#     user='root',
#     database='bc_registry_db',
#     password='actowiz',
#     charset='utf8mb4',
#     autocommit=True,
#     client_flag=CLIENT.MULTI_STATEMENTS
# )
# if client.open:
#     print('Database connection Successful!')
# else:
#     print('Database connection Un-Successful.')
# cursor = client.cursor()
#
# cursor.execute(pincodes_create_query)
# cursor.execute(bc_registry_create_query)
#
# count = 0
# with open('pincodes.txt', 'r') as file:
#     data_list = file.readlines()
#     for data in data_list[1:]:
#         insert_query = f'''INSERT INTO pincodes (pincode) VALUES (%s);'''
#         cursor.execute(insert_query, args=(data,))
#         print(count)
#         count += 1
