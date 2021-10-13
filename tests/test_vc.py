import json
import logging
import os
import traceback
from pprint import pprint
from typing import Any, Dict, Iterable, List, Set

from mysql.connector import connect

import pandas as pd
from mysql_simplelib import User, Server, Database, Table

# Config logger
format='%(asctime)s %(name)-12s %(funcName)-12s %(levelname)-8s %(message)s'
logging.basicConfig(level='DEBUG', format=format)
logger = logging.getLogger(__name__)

def main(server, conn):

    logs_list = get_logs(server, conn)

    # Filter logs by country: only UK and The Netherlands
    filt_log_list = []
    for log in logs_list:
        vc_action_value = log['vc_action_value']
        if vc_action_value['country'] in ('uk', 'nl'):
            filt_log_list.append(log)
    logs_list = filt_log_list

    # Filter logs by category: only cellphones and tablets
    filt_log_list = []
    for log in logs_list:
        vc_action_value = log['vc_action_value']
        if vc_action_value['cid'] in (4364, 431006):
            filt_log_list.append(log)
    logs_list = filt_log_list

    distinct_offer_ids = get_unique_offer_ids(logs_list)

    offers_dict = get_offers(server, conn, distinct_offer_ids)

    # Add shop status to the log offers
    for log in logs_list:
        top_results = log['vc_action_value']['top_results']
        # If the offer is in the shop_prices table, add the shop
        # status to the log offer (top_results item)
        for log_offer in top_results:
            log_offer_id = log_offer['offer_id']
            if log_offer_id not in offers_dict:
                continue
            offer = offers_dict[log_offer_id]
            log_offer["shop_status"] = offer["shop_status_default"]
            log_offer.update(offer)

    # Filter results by shop status: Only keep the logs where the first
    # two top results are from recommended shops.
    filt_logs = []
    for log in logs_list:
        vc_action_value = log['vc_action_value']
        top_results = vc_action_value['top_results']
        # Get the stati of the first two offers
        shops_stati = [offer.get('shop_status', 0) for offer in top_results[:2]]
        offers_are_from_rec_shops = all(map(lambda x: x==1, shops_stati))
        if offers_are_from_rec_shops:
            filt_logs.append(log)

    pprint(filt_logs)
    print(len(logs_list), len(filt_logs))


def get_unique_offer_ids(logs_list: List[Dict[str, Any]]) -> Set[int]:

    # Get unique offer ids
    offer_ids = set()
    for row in logs_list:
        row_offers = row['vc_action_value']['top_results']
        row_offer_ids = [offer['offer_id'] for offer in row_offers]
        for offer_id in row_offer_ids:
            offer_ids.add(str(offer_id))

    return offer_ids


def get_offers(server, conn, offer_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    """
    Get the offers data from `valuechecker.shops_prices` for the offers
    that appear in the logs_list
    """

    # Get offers
    query = """
        SELECT
            sp.offer_id,
            sp.offer_url,
            sp.deeplink,
            sp.pid,
            sp.source_shop_id,
            sp.total_price,
            sp.currency,
            sp.international,
            COALESCE(s.shop_name, ss.source_shop_name) AS shop_name,
            IFNULL(s.shop_status_default, 0) AS shop_status_default
        FROM valuechecker.shops_prices sp
        LEFT JOIN valuechecker.shops_sources ss ON ss.source_shop_id = sp.source_shop_id
        LEFT JOIN valuechecker.shops s ON s.shop_id = ss.shop_id
        WHERE sp.offer_id IN %s
        ;
    """
    offer_ids_str = "(" + ", ".join(offer_ids) + ")"
    query = query % offer_ids_str
    cursor = server.execute(conn, query)
    rows_list: List[dict] = cursor.fetchall()
    cursor.close()

    # Parse into nested dicts
    offers_dict = dict()
    for row in rows_list:
        offers_dict[row["offer_id"]] = row

    # Done
    return offers_dict


def get_logs(server, conn):
    """
    Get logs for RP Searches where the two top results (offers) are for
    the same pid but the total price of the 2nd is quite lower than the
    1st.
    """
    # # Select records
    # query = """
    #     SELECT *
    #     FROM (
    #         SELECT
    #             val.uuid,
    #             val.create_time,
    #             val.user_id,
    #             val.vc_action_value,
    #             val.vc_action_value -> '$.top_results[0].pid' AS pid_0,
    #             val.vc_action_value -> '$.top_results[0].offer_id' AS offer_id_0,
    #             val.vc_action_value -> '$.top_results[0].total_price' AS total_price_0,
    #             val.vc_action_value -> '$.top_results[1].pid' AS pid_1,
    #             val.vc_action_value -> '$.top_results[1].offer_id' AS offer_id_1,
    #             val.vc_action_value -> '$.top_results[1].total_price' AS total_price_1
    #         FROM valuechecker.vc_action_log val
    #         WHERE val.vc_action_id = 5 AND val.create_time > NOW() - INTERVAL 4 DAY
    #     ) rplogs
    #     WHERE 1
    #         AND rplogs.pid_0 = rplogs.pid_1
    #         AND rplogs.total_price_0 < 0.7 * rplogs.total_price_1
    #     LIMIT 5000
    #     ;
    # """

    query = """
        SELECT *
        FROM (
            SELECT
                val.uuid,
                val.create_time,
                val.user_id,
                val.vc_action_value,
                val.vc_action_value -> '$.top_results[0].pid' AS pid_0,
                val.vc_action_value -> '$.top_results[0].offer_id' AS offer_id_0,
                val.vc_action_value -> '$.top_results[0].total_price' AS total_price_0,
                val.vc_action_value -> '$.top_results[1].pid' AS pid_1,
                val.vc_action_value -> '$.top_results[1].offer_id' AS offer_id_1,
                val.vc_action_value -> '$.top_results[1].total_price' AS total_price_1,
                val.vc_action_value -> '$.top_results[2].pid' AS pid_2,
                val.vc_action_value -> '$.top_results[2].offer_id' AS offer_id_2,
                val.vc_action_value -> '$.top_results[2].total_price' AS total_price_2
            FROM valuechecker.vc_action_log val
            WHERE val.vc_action_id = 5 AND val.create_time > NOW() - INTERVAL 4 DAY
        ) rplogs
        WHERE 1
        	AND rplogs.pid_0 = rplogs.pid_1
        	AND rplogs.pid_0 = rplogs.pid_2
            AND ABS(rplogs.total_price_0 - rplogs.total_price_1) / rplogs.total_price_0 > 0.2
            AND ABS(rplogs.total_price_1 - rplogs.total_price_2) / rplogs.total_price_1 < 0.1
        LIMIT 1000
        ;
    """
    cursor = server.execute(conn, query)
    rows_list: List[dict] = cursor.fetchall()
    cursor.close()

    # Deserialize vc_action_values (str -> dict).
    int()
    for row in rows_list:
        vav_str: str = row['vc_action_value']
        vav_dict: dict = json.loads(vav_str)
        row["vc_action_value"] = vav_dict

    # Done
    return rows_list


def get_connection():
    # ---
    # USER
    here = os.path.dirname(__file__)
    dotenvPath = os.path.join(here, '.env')
    user = User.from_dotenv(dotenvPath)
    # ---
    # SERVER ACTIONS
    # Create Server object
    server = Server(host='network-controller.sto.alatest.se', port=3306)
    connection = server.connect(user)
    return server, connection


if __name__ == '__main__':
    try:
        server, conn = get_connection()
        main(server, conn)
    except Exception as e:
        logger.critical(e.__repr__())
        print(traceback.format_exc())
    finally:
        conn.close()
