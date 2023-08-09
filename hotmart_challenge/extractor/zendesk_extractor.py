import requests

class ZendeskTicketsExtractor():
    """Holds the logic to extract data from the "tickets/" endpoint of the API.

    This endpoint contains all tickets on zendesk API, the pagination is done by
        page_keys "next_page", and the number of results by key "per_page".
        Basic auth is used to authorize the extraction, using user and token.

    Args:
        base_url (str): Base API url.
        user (str): API email followed by argument "/token".
            example: email@email.com/token.
        token (str): API auth token.
        limit (int, optional): Number of results per page. Defaults to 100.
        tries (int, optional): Number of retries if the request fails. Defaults to 1.
    """

    def __init__(
        self, base_url: str, user: str, token: str, limit: int = 100, tries: int = 1
    ):
        self.base_url = base_url
        self.user = user
        self.token = token
        self.limit = limit
        self.tries = tries

    def extract(self):
        """Yields the API response for this endpoint.
        In this case, yields a dictionary containing a list of results, where each
        result is an user registration data.

        Yields:
            dict: Json containing the API results
        """
        for result in requests.get(
            url=self.base_url,
            path="api/v2/tickets.json",
            auth=(
                self.user,
                self.token,
            ),
            payload={"per_page": self.limit},
            tries=self.tries,
            pagination=True,
            page_keys=("next_page",),
        ):
            yield {"data": result["tickets"]}
