import json
import os
from urllib.parse import urljoin

import pika
import requests
from dotenv import load_dotenv

load_dotenv()


def get_keycloak_token():
    """
    Retrieves an access token from Keycloak using the Resource Owner Password Credentials flow.
    
    Raises:
        ValueError: If any required Keycloak environment variable is missing.
        HTTPError: If the token request fails.
    
    Returns:
        The access token as a string.
    """
    token_url = os.environ.get("KEYCLOAK_TOKEN_URL")
    client_id = os.environ.get("KEYCLOAK_CLIENT_ID")
    username = os.environ.get("ADMIN_USERNAME")
    password = os.environ.get("ADMIN_PASSWORD")

    if not all([token_url, client_id, username, password]):
        raise ValueError("Missing required Keycloak environment variables.")

    data = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
    }

    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    token_data = resp.json()
    return token_data["access_token"]


def get_candidate_search_status(organization_id, experiment_id, token):
    """
    Retrieves the candidate search status for a given organization and experiment.
    
    Args:
        organization_id: The unique identifier of the organization.
        experiment_id: The unique identifier of the experiment.
        token: Bearer access token for API authentication.
    
    Returns:
        The JSON response containing the candidate search status.
    """
    base_url = os.environ.get("API_BASE_URL", "http://localhost")
    endpoint = f"/api/fedml-experiments/{organization_id}/{experiment_id}/candidate_search_status"
    url = urljoin(base_url, endpoint)

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def publish_to_rabbitmq(message):
    """
    Publishes a Python dictionary message to a RabbitMQ queue.
    
    The queue name, host, and port are determined by environment variables. The message is serialized to JSON and sent with persistent delivery mode.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5672"))
    queue_name = os.environ.get("RABBITMQ_QUEUE", "queue_A")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    body = json.dumps(message)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )

    print(
        f"[Producer] Published to {queue_name} on {rabbitmq_host}:{rabbitmq_port} => {body}"
    )
    connection.close()


def main():
    """
    Coordinates authentication, API querying, and message publishing for candidate search status.
    
    Retrieves organization and experiment IDs from environment variables, obtains a Keycloak access token, fetches candidate search status from the API, and publishes the result to a RabbitMQ queue. Raises a ValueError if required environment variables are missing.
    """
    organization_id = os.environ.get("ORGANIZATION_ID")
    experiment_id = os.environ.get("EXPERIMENT_ID")

    if not organization_id or not experiment_id:
        raise ValueError(
            "Missing ORGANIZATION_ID or EXPERIMENT_ID in environment variables."
        )

    token = get_keycloak_token()
    status_response = get_candidate_search_status(organization_id, experiment_id, token)
    publish_to_rabbitmq(status_response)


if __name__ == "__main__":
    main()
