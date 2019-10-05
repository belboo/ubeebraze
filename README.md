# Simple Braze API Client

This is a simple Braze API client for updating user data and sending track events for CRM.
Idea is to use it as a sink for identify and track calls without managing the API calls directly.
The definition of the API allows for one call to contain max 75 user objects and 75 track events.
This is taken into account and events and users are accumulated in the queue and flushed as necessary.

## Contents:

### Braze Config

Holds basic configuration for the client
Todo: add support for environment vars and read from file

### User Class

Class abstracting a user object in Braze. Holds arbitrary traits.

### Event Class

Class abstracting an event. Can hold arbitrary properties dictionary.

### BrazeClient

Braze client to abstract the API interaction.

## Example

```python
import ubeebraze

braze = ubeebraze.braze.BrazeClient(api_key=API_KEY, batch_size=75, send=True)

users = get_users()

for u in users:
    braze_user = braze.user(
        str(u.external_id),
        country = u.country,
        email = u.email,
        custom_property = u.custom_property)

    braze_user.enqueue()

    braze.event(
        external_id=str(u.external_id),
        name='User Signup',
        time=u.created_at,
        birthdate=u.birthdate,
        isB2b=u.is_b2b,
        status=u.status
        ).enqueue()

braze.flush()
```

## Todo
* replace the stupid pause property with proper rate-limiter (probably unnecessary)
* replace the local file log with a logging logger
* add schema dictionary
* add Pandas DF ingestion functions
* add TTL for flushing
