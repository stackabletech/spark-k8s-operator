#!/usr/bin/env python3
import requests


def check_sent_events():
    response = requests.post(
        "http://spark-vector-aggregator:8686/graphql",
        json={
            "query": """
                {
                    transforms(first:100) {
                        nodes {
                            componentId
                            metrics {
                                sentEventsTotal {
                                    sentEventsTotal
                                }
                            }
                        }
                    }
                }
            """
        },
    )

    assert response.status_code == 200, (
        "Cannot access the API of the vector aggregator."
    )

    result = response.json()

    transforms = result["data"]["transforms"]["nodes"]
    for transform in transforms:
        sentEvents = transform["metrics"]["sentEventsTotal"]
        componentId = transform["componentId"]

        if componentId == "filteredInvalidEvents":
            assert sentEvents is None or sentEvents["sentEventsTotal"] == 0, (
                "Invalid log events were sent."
            )
        else:
            assert sentEvents is not None and sentEvents["sentEventsTotal"] > 0, (
                f'No events were sent in "{componentId}".'
            )


if __name__ == "__main__":
    check_sent_events()
    print("Test successful!")
