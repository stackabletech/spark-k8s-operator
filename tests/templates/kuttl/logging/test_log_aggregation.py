import json
import subprocess


def check_sent_events():
    response = subprocess.run(
        [
            "grpcurl",
            "-plaintext",
            "-d",
            '{"limit": 100}',
            "spark-vector-aggregator:8686",
            "vector.observability.v1.ObservabilityService/GetComponents",
        ],
        capture_output=True,
        text=True,
        check=True,  # Raise a CalledProcessError if non-zero return
        timeout=20,  # seconds
    )
    result = json.loads(response.stdout)
    components = result.get("components", [])
    transforms = [
        c for c in components if c.get("componentType") == "COMPONENT_TYPE_TRANSFORM"
    ]

    assert len(transforms) > 0, "No transform components found"

    for transform in transforms:
        sentEvents = transform["metrics"]["sentEventsTotal"]
        componentId = transform["componentId"]

        if componentId == "filteredInvalidEvents":
            assert sentEvents is None or int(sentEvents) == 0, (
                "Invalid log events were sent."
            )
        else:
            assert sentEvents is not None and int(sentEvents) > 0, (
                f'No events were sent in "{componentId}".'
            )


if __name__ == "__main__":
    check_sent_events()
    print("Test successful!")
