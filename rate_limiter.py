class RateLimiter:
    """
    A rate limiter tracks requrest made by each user and ensures they 
    do not exceed a configured number of requrest within a given time window
    When a requrest arrives, it shoudl determine whether the user is still within
    their limit and either allow or reject the request

    More info:

    max of 3 request every 5 seconds
    """

    def __init__(self,max_request:int,window_seconds:int):
        # TODO
        pass

    def allow(self,user_id:str,timestamp:int)-> bool:
        # TODO: return True if request is allowed for user, False otherwise
        pass

# TODO: Write some test cases