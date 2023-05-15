import requests
import logging
import json
import backoff

API_URL = 'https://api.svt.se'
QUERY = """query {
    genresSortedByName {
        genres {
            id
            name
            description
        }
    }
    allEpisodesForInternalUse(include: active) {
        videoSvtId
        urls {
            svtplay
        }
    }
    programAtillO (filter: {includeFullOppetArkiv: true}) {
        flat {
            name
            episodes {
                videoSvtId
                name
                duration
                validFrom
                validTo
                restrictions {
                    onlyAvailableInSweden
                }
                shortDescription
                longDescription
                productionYear
                genres {
                    id
                }
            }
            urls {
                svtplay
            }
        }
    }
}"""

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=300)
def fetch_graphql():
    resp = requests.post(f'{API_URL}/contento/graphql', json={'query': QUERY})
    resp.raise_for_status()
    return json.loads(resp.text)

def download(database):
    try:
        raw = fetch_graphql()
        if errors := raw.get('errors'):
            logging.error("GraphQL query failed with %s",
                          ' | '.join(error['message'] for error in errors))
        else:
            database.update(raw)
    except requests.exceptions.RequestException as ex:
        logging.error("GraphQL query failed with %s - exhausted retries", ex)
