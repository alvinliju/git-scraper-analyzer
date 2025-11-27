- switched from implementing a token bucket to measuring rate limits from github api itself it has a header that returns us how many requests are left and when the rate limit resets

- we are now creating client sesion for all the requests which is slow becasue:
  - dns lookup
  - tcp handshake
  - ssl handshake
  - http request

but if we could create a client session once and reuse it for all requests, we could avoid the overhead of creating a new session for each request.

we created a client session once and instead of resuing i did something stupid:
i did async with self.session as session:

but all i wanted to do was reuse the session but i created session again and closed it immeditely thats why we got session is close error
