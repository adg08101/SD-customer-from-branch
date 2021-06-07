import pymysql

HOST = ''
USER = ''
PASSWORD = ''
DB = ''
PORT = 0
CONNECTION = False
NEW_CONNECTION = False

datetime_format = "'%h:%i:%s %p')"


def main_proc(p_host=HOST, p_user=USER, p_password=PASSWORD, p_db=DB, p_port=PORT, p_connection=CONNECTION):
    print("Connecting...")
    if not p_connection:
        connection = pymysql.connect(host=p_host,
                                     user=p_user,
                                     password=p_password,
                                     database=p_db,
                                     port=int(p_port),
                                     cursorclass=pymysql.cursors.DictCursor)
        global HOST, USER, PASSWORD, DB, PORT, CONNECTION, NEW_CONNECTION
        HOST = p_host
        USER = p_user
        PASSWORD = p_password
        DB = p_db
        PORT = p_port
        CONNECTION = connection
    else:
        connection = CONNECTION

    print("Done... please wait for data retrieval...")

    with connection.cursor() as cursor:
        sql = f"SELECT org_name, legacy_org_name, sd_db_source FROM {p_db}.orgs ORDER BY org_name ASC;"
        cursor.execute(sql)
        organizations = cursor.fetchall()

        organizations_to_delete = list()
        branches = list()

        for organization in range(len(organizations)):
            sql = (
                f"SELECT b.BRANCH_NAME FROM {organizations[organization]['sd_db_source']}"
                f".branch AS b "
            )

            try:
                cursor.execute(sql)
                branch = cursor.fetchall()
                if not branch:
                    organizations_to_delete.append(organizations[organization]['org_name'])
                else:
                    branches.append(str(organizations[organization]) + str(branch))
            except pymysql.err.ProgrammingError:
                organizations_to_delete.append(organizations[organization]['org_name'])

        cursor.close()

    for organization in range(len(organizations)):
        if organizations[organization]['org_name'] in organizations_to_delete:
            organizations[organization]['show'] = 'false'
        else:
            organizations[organization]['show'] = 'true'

    organizations_temp = list()

    for organization in range(len(organizations)):
        if organizations[organization]['show'] == 'true':
            organizations_temp.append(organizations[organization])

    for branch in range(len(branches)):
        print(branches[branch])


def get_input(text, min_value, max_value):
    while True:
        value = input(text + ": ")

        try:
            if int(value) < min_value or int(value) > max_value or not value.isdigit():
                raise ValueError("value_error")
            else:
                return int(value)
        except ValueError:
            print("Input value error")


if __name__ == '__main__':
    while True:
        host = input("HOST ('smartdrops.gsoftinnovation.net'): ")
        db = input("DB ('smartconnect'): ")
        port = input("PORT (3306):")
        user = input("USER ('root'):")
        password = input("PASS: ")

        if host == '':
            host = 'smartdrops.gsoftinnovation.net'

        if db == '':
            db = 'smartconnect'

        if port == '':
            port = 3306

        if user == '':
            user = 'root'

        try:
            main_proc(host,
                      user,
                      password,
                      db,
                      port,
                      p_connection=False)
            break
        except (pymysql.err.OperationalError, ValueError) as err:
            print("Connection error, could not connect to DB")
            print(err)
