import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import urllib3
import ssl
from urllib3.util import Retry
from datetime import datetime, timedelta
import sys


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

days_back = 365
com_to_monitor = "易方达"

max_retries = 3

timeout = 3

retry_strategy = Retry(
    total=max_retries,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1,  # You can adjust the backoff factor as needed
)


def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")


def date_to_timestamp(date_str, date_format="%Y-%m-%d %H:%M:%S"):
    dt_object = datetime.strptime(date_str, date_format)
    timestamp = dt_object.timestamp()
    return int(timestamp * 1000)


def extract_com_name(com_name):
    if com_name:
        return (
            com_name.replace("基金管理有限公司", "")
            .replace("基金管理股份有限公司", "")
            .replace("基金管理", "")
            .replace("有限公司", "")
            .replace("股份有限公司", "")
        )
    return "其他"


def simple_message(user):
    creationDate = user["personCertHistoryList"][0]["creationDate"].split(" ")[0]
    if len(user["personCertHistoryList"]) == 1:
        return (
            f"{user['userName']}：{extract_com_name(user['orgName'])}, {creationDate}"
        )
    else:
        return f"{user['userName']}：{extract_com_name(user['personCertHistoryList'][1]['orgName'])}->{extract_com_name(user['personCertHistoryList'][0]['orgName'])}, {creationDate}"


def extract_user_data(data):
    core_data = {
        "userName": data["userName"],
        "sex": data["sex"],
        "orgName": data["orgName"],
        "educationName": data["educationName"],
        "personCertHistoryList": [
            {
                "orgName": item["orgName"],
                "statusName": item["statusName"],
                "creationDate": timestamp_to_date(item["creationDate"]),
            }
            for item in data["personCertHistoryList"]
        ],
    }
    return core_data


class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
session = requests.Session()
session.mount("https://", CustomHttpAdapter(ctx))


def make_request_with_retry(
    url,
    method="get",
    data=None,
    json=None,
    headers=None,
    max_retries=max_retries,
    cookies=None,
    timeout=timeout,
):
    """
    Attempts to make an HTTP request (GET or POST) to the specified URL with a given timeout
    and retries up to a maximum number of times if timeouts occur.

    Parameters:
    - url: The URL to request.
    - method: The HTTP method to use ('get' or 'post').
    - data: The data to send in the body of the request (for POST).
    - json: A JSON serializable Python object to send in the body of the request (for POST).
    - headers: Dictionary of HTTP Headers to send with the request.
    - max_retries: The maximum number of retry attempts.
    - timeout: The timeout for each request in seconds.

    Returns:
    - The response object if the request was successful.
    - None if all retries failed.
    """
    for attempt in range(max_retries):
        try:
            if method.lower() == "post":
                response = session.post(
                    url,
                    data=data,
                    json=json,
                    headers=headers,
                    timeout=timeout,
                    cookies=cookies,
                )
            else:
                response = session.get(
                    url, headers=headers, cookies=cookies, timeout=timeout
                )
            return response  # Return immediately if successful
        except requests.exceptions.Timeout:
            sys.stderr.write(f"Request timed out on attempt {attempt + 1}.")
        except requests.exceptions.RequestException as e:
            sys.stderr.write(f"Request failed with error: {e}")
            return None  # Return None if a non-timeout exception occurs

    sys.stderr.write("Request failed after retrying")
    return None  # Return None if all retries failed due to timeout


def get_company_scale_rank():
    url = f"https://www.amac.org.cn//portal/front/statics/fundIndData/findFundManageOrgans?year=&quarter="
    response = make_request_with_retry(url).json()
    res = {}
    for it in response["data"]["data"]["amsomcfofmiVOs"]:
        res[it["companyName"]] = it["ranking"]
    return res


def get_company_list():
    all_list = []
    for i in range(10):
        url = f"https://gs.amac.org.cn/amac-infodisc/api/pof/personOrg?rand=0.8364584791368805&page={i}&size=20&rand=0.016298248893151346"
        params = {"orgType": "gmjjglgs", "page": i}
        cookies = {
            "Hm_lvt_a0d0f99af80247cfcb96d30732a5c560": "1710400957",
            "Hm_lpvt_a0d0f99af80247cfcb96d30732a5c560": "1710404853",
        }
        headers = {
            "Content-Type": "application/json",
            "Host": "gs.amac.org.cn",
            "Origin": "https://gs.amac.org.cn",
            "Referer": "https://gs.amac.org.cn/amac-infodisc/res/pof/person/personOrgList.html",
            "Sec-Ch-Ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "linux",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        response = make_request_with_retry(
            url, method="post", json=params, headers=headers, cookies=cookies
        )
        result = response.json()
        list_ = result.get("content", [])
        if not list_:
            break
        all_list.extend(list_)
    return all_list


def get_user_list(user_id):
    all_list = []
    maxPages = 100
    for i in range(20):
        if i > maxPages:
            break

        url = f"https://gs.amac.org.cn/amac-infodisc/api/pof/person?rand=0.8364584791368805&page={i}&size=100"
        params = {"userId": user_id, "page": 1}
        cookies = {
            "Hm_lvt_a0d0f99af80247cfcb96d30732a5c560": "1659423351,1660821735,1661239152",
            "Hm_lpvt_a0d0f99af80247cfcb96d30732a5c560": "1661475181",
        }
        headers = {
            "Host": "gs.amac.org.cn",
            "Origin": "https://gs.amac.org.cn",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        response = make_request_with_retry(
            url,
            "post",
            json=params,
            headers=headers,
            cookies=cookies,
        )
        try:
            result = response.json()
            maxPages = result["totalPages"]
            list_ = result.get("content", [])
            if not list_:
                break
        except Exception as e:
            continue
        all_list.extend(list_)
    return all_list


def is_recent_leave(user):
    now = datetime.now()

    # One day before the current time
    one_day_before = now - timedelta(days=days_back)

    # Convert to timestamps (assuming you want seconds since the epoch)
    one_day_before_timestamp = int(one_day_before.timestamp()) * 1000
    return user["personCertHistoryList"][0]["creationDate"] >= one_day_before_timestamp


def main():
    result = []
    companies = get_company_list()
    rank = get_company_scale_rank()
    sorted_companies = sorted(
        companies,
        key=lambda x: (rank.get(x["orgName"], 100), -x["operNum"]),
    )
    for company in sorted_companies[:30]:
        # Assuming `userId` is an attribute of the `Company` class
        if com_to_monitor in company["orgName"]:
            continue
        users = get_user_list(company["userId"])
        for user in users:
            # Assuming `certStatusChangeTimes` and `getPersonCertHistoryList` are attributes/methods of the `User` class
            if (
                user["certStatusChangeTimes"] > 2
                and user["personCertHistoryList"][1]["orgName"]
                and com_to_monitor in user["personCertHistoryList"][1]["orgName"]
                and is_recent_leave(user)
            ):
                result.append(extract_user_data(user))
    result = sorted(
        result,
        key=lambda x: x["personCertHistoryList"][0]["creationDate"],
        reverse=True,
    )

    return [simple_message(i) for i in result]


if __name__ == "__main__":
    print(main())
