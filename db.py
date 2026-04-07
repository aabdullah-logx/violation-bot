import pymysql
import time
import db_settings
from sshtunnel import SSHTunnelForwarder

def connect_to_db():
    tunnel = None
    connection = None

    try:
        # Check if the tunnel has already been established
        if not tunnel:
            # SSH tunnel configuration
            tunnel = SSHTunnelForwarder(
                ('3.21.248.179', 22),  # SSH server IP and port
                ssh_username='ubuntu',
                ssh_pkey='app-api-ful-feb-17-2023.pem',
                remote_bind_address=('aie-test-environment.cluster-cgmpmippwljm.us-east-2.rds.amazonaws.com', 3306),  # Remote MySQL server address and port
                local_bind_address=('127.0.0.1', 3314)  # Local port for SSH tunnel
            )
            # Start the SSH tunnel
            tunnel.start()
        
        # MySQL database connection
        db_config = {
            'host': '127.0.0.1',  # Use local host for MySQL connection
            'user': db_settings.user,
            'password': db_settings.password,
            'db': db_settings.db,
            'port': 3314,  # Local port for SSH tunnel
            'local_infile': db_settings.local_infile
        }
        
        # Connect to the database through the SSH tunnel
        connection = pymysql.connect(**db_config)
    except Exception as e:
        # Handle any exceptions that occur during tunnel setup or database connection
        print(f"An error occurred: {e}")
        if tunnel:
            tunnel.stop()  # Close the tunnel if it was started
    return connection, tunnel


# # Database connection setup
# def connect_to_db():
#     return pymysql.connect(
#         host=db_settings.host, 
#         user=db_settings.user, 
#         password=db_settings.password, 
#         db=db_settings.db, 
#         port=db_settings.port,
#         local_infile=db_settings.local_infile
#     )


def insert_health(health):
    try:
        connection, tunnel = connect_to_db()
    except Exception as e:
        print(f'{e}')


    # SQL statement for inserting data
    sql = """
    INSERT  INTO `account_health`
    (`storename`, `status`, `health_rating`, `ODR`, `VTR`, `Buybox`, `balance`, `negative_feedback`, `a_to_z_claims`, `chargeback_claims`, `late_shipment_rate`,`pre_fulfilment_cancel_rate`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare the data for bulk insertion
    data = (health['storename'], health['status'], health['health_rating'], health['odr'], health['vtr'], health['buybox'], health['balance'], health['negative_feedback'], health['a_to_z_claims'], health['chargeback_claims'], health['late_shipment_rate'], health['pre_fulfilment_cancel_rate'],)

    try:
        with connection.cursor() as cursor:
            # Execute the SQL statement in bulk
            cursor.execute(sql, data)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Rollback in case of any error
    finally:
        connection.close()
        if tunnel:
            tunnel.stop()

def insert_violations(violations):
    try:
        connection, tunnel = connect_to_db()
    except Exception as e:
        print(f'{e}')


    # SQL statement for inserting data
    sql = """
    INSERT IGNORE INTO `listing_issues_sc`
    (`storename`, `asin`, `impact`, `action_taken`, `reason`, `publish_time`, `category`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare the data for bulk insertion
    data = [(violation['storename'], violation['asin'], violation['impact'],
             violation['action_taken'], violation['reason'], violation['publish_time'], violation['category'])
            for violation in violations]

    try:
        with connection.cursor() as cursor:
            # Execute the SQL statement in bulk
            cursor.executemany(sql, data)
        # Commit the transaction
        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Rollback in case of any error
    finally:
        connection.close()
        if tunnel:
            tunnel.stop()


def get_distinct_storenames():
    try:
        connection, tunnel = connect_to_db()
    except Exception as e:
        print(f'{e}')
        return None
    
    # SQL statement to count rows
    sql = "SELECT DISTINCT(storename) FROM listing_issues_sc;"
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            storenames = [item[0] for item in result]
            return storenames  # Access the first item in the tuple for the count
    except Exception as e:
        print(f"An error occurred while fetching row count: {e}")
        return None
    finally:
        connection.close()
        if tunnel:
            tunnel.stop()


def main():
    # while True:
    stores_scraped = get_distinct_storenames()
    print(f"Total rows in listing_issues_sc: {stores_scraped}")
        # time.sleep(20)


if __name__ == "__main__":
    main()
