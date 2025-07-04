from apify_client import ApifyClient
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener API key desde variables de entorno
apify_token = os.getenv('APIFY_API_KEY')
if not apify_token:
    print("Error: No se encontró la variable de entorno APIFY_API_KEY")
    exit()

# Initialize the ApifyClient with your API token
client = ApifyClient(apify_token)
url ="https://www.linkedin.com/in/ariel-yance-orozco/"
run_input = {
    "cookie": [
        {
            "name": "li_sugr",
            "value": "40510076-a6b0-4cd5-965c-b6acac644ce3",
            "domain": ".linkedin.com",
            "path": "/",
            "expires": "2025-08-14T21:56:52.261Z"
        },
        {
            "name": "li_rm",
            "value": "AQE8rvNrfDmzSAAAAZcgA-NaE_kga5o_qWmXs9CEtelSeBGBLK-3oj9qg30VHw_OTgBvfR3Q1nBkVMhgSZ_APhz3y_FutywmDByyX2-2HR7TjjckBI9ST1bA",
            "domain": ".www.linkedin.com",
            "path": "/",
            "expires": "2026-06-05T13:38:55.703Z"
        },
        {
            "name": "li_at",
            "value": "AQEDATwq6g0FCdqOAAABl0BQVFgAAAGXZFzYWE4AIVETkPmGP5nTaxFMwBP1-wEFEiknikFBo2oeguQVv2H7gv_2luRY9-Gcp6q7h2NSJ80zosMFaIU23K2BTAXhChCwEJ41LYfWH0TN70uD-75I1gus",
            "domain": ".www.linkedin.com",
            "path": "/",
            "expires": "2026-06-05T13:38:55.703Z"
        },
        {
            "name": "lang",
            "value": "v=2&lang=es-es",
            "domain": ".linkedin.com",
            "path": "/"
        },
        {
            "name": "g_state",
            "value": "{\"i_l\":0}",
            "domain": "www.linkedin.com",
            "path": "/",
            "expires": "2025-12-02T13:38:17.000Z"
        },
        {
            "name": "fptctx2",
            "value": "taBcrIH61PuCVH7eNCyH0J9Fjk1kZEyRnBbpUW3FKs9EWYO8RHHrIbqKFeT8Mw2Cdl3N1Amqkcc4n3lg%252b15wdBj2tiRSrp1luzLiVnKFEA4FE9uv7LDcL36cAYrIf3zMiKzuzcnyJp7qRvLYXpGE8KNsLeL0vMoi9pl86YPyRdvrZNyPCe9RVVPAnYhqEMYnyMk1UIhwi7OLt08aR4nxsB5%252fez8F1U%252f26kp7LS0%252bE5%252fWgdX82P8oUYNOC5gu1T4zAT3C3w7BuOpi%252febw7Ja0%252fQKtwi%252f1dinSQf%252bu2ZoK7P10YxoAVBGoEcRlJzEyq9I5u49Row4fjSTkyrRq0e4KWRJvXoF7TcR7gp2oGgG4rLk%253d",
            "domain": ".linkedin.com",
            "path": "/"
        },
        {
            "name": "dfpfpt",
            "value": "3371bac7e95e4097808749970b9d2f2c",
            "domain": ".linkedin.com",
            "path": "/",
            "expires": "2026-05-30T05:38:47.363Z"
        },
        {
            "name": "bscookie",
            "value": "\"v=1&202505160118306e22a519-8b06-469f-8a01-bb887df11e0fAQFdqr5BpJpG8_k6YBtDhpb99qKlrwr7\"",
            "domain": ".www.linkedin.com",
            "path": "/",
            "expires": "2026-06-05T13:38:55.703Z"
        },
        {
            "name": "bcookie",
            "value": "\"v=2&b8a5f0e9-560e-41c0-8ff0-2a029f6ea165\"",
            "domain": ".linkedin.com",
            "path": "/",
            "expires": "2026-06-05T15:20:09.598Z"
        },
        {
            "name": "UserMatchHistory",
            "value": "AQL0AuoXO_8d2wAAAZdArQyfl4RPqDCYsDKWjk8LayNFSJGJW58RKhI72Z_QjVOA_FU9KUGTDU4sqrqjNB-IqfdXJSEbuZC1MgsKB1RVPRbT6G5YBd_dzsgqISAq7BhvZghiLMgiTP9zeYKqlf36pKADOT_-ahzahF_ZW03DmK8i7wJqGWvn2tHgQUl0NlEtlD-bza_EUNvuplWUVM5-h9KdyO4Ipn95mDmWgmQAnJqek0meolSX7V-OpiYObACsQwsyK4iy_fxfQ5olewwTF4QCFWKGcZu0fF2Fm7IgD3PTpFxgKbQeMFE3_UjsqM7d8FW7ZuEaoL9lPEj8KLBC5fLyjcJoLW1n60GwIgS5Y-1GNfPjaQ",
            "domain": ".linkedin.com",
            "path": "/",
            "expires": "2025-07-05T15:20:12.000Z"
        },
        {
            "name": "JSESSIONID",
            "value": "\"ajax:2195909279237352274\"",
            "domain": ".www.linkedin.com",
            "path": "/",
            "expires": "2025-09-03T13:38:55.703Z"
        },
        {
            "name": "AnalyticsSyncHistory",
            "value": "AQICwQ_c4dzkIAAAAZbWq-ClghpGQIv78WEqW1GvaKZdUo02KacciXfINXhd9JpyOswD73CZCJ0nqCILv6VNJg",
            "domain": ".linkedin.com",
            "path": "/",
            "expires": "2025-06-15T01:19:24.903Z"
        }
    ],
    "findContacts": True,
    "maxDelay": 60,
    "minDelay": 15,
    "proxy": {
        "useApifyProxy": True,
        "apifyProxyGroups": [
            "RESIDENTIAL"
        ],
        "apifyProxyCountry": "CO"
    },
    "scrapeCompany": False,
    "urls": [
        url
    ]
}
 


run = client.actor("PEgClm7RgRD7YO94b").call(run_input=run_input)


for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    print(item)