"""The proof of work a question is to carry: ALTCHA, on the server.

Turnstile goes, since Cloudflare says it is not supported in mainland
China (#128, section 2.8, and the owner's decision on Q6). In its place
is ALTCHA (https://github.com/altcha-org/altcha), self-hosted: the page
is to solve a challenge in a Web Worker, and send the solution with its
question.

  - `issue` makes a challenge for one client, signed, with its expiry
    and the client's key among the signed parameters (`data.client`),
    so that neither can be changed to suit the client.
  - `verify` checks, once a question, the signature, the solution, the
    expiry, the client, and that the challenge was not used before.
    Nothing calls it yet: the chat will.

A proof of work prices automation; it does not identify people. The
daily spend cap is the real bound.

The protocol is ALTCHA's PoW v2 in deterministic mode, through its own
library (`altcha` on PyPI, by ALTCHA's author, MIT). The server draws a
counter, derives a key from it, and signs the key's first half into the
challenge as the prefix to find, and a signature of the whole key
beside it. The client derives keys from counter 0 up until one starts
with the prefix, so the work is the counter. Issuing derives one key;
verifying the key a client found takes two HMACs, and no derivation.

What the library does not do, this does:
  - The payload is checked for its shape before the library reads it.
    The library raises on some malformed payloads rather than refusing
    them: a negative counter, a key or a signature that is not hex, an
    expiry that is not a number.
  - The expiry is checked against the service's clock as well.
  - The client, and single use. A used challenge's nonce is kept in
    web.sqlite until the challenge expires, so a restart does not make
    it good again, and one insert decides between two uses at once.
"""
import base64
import enum
import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Any
from typing import TypeGuard

import structlog
from altcha import create_challenge
from altcha import Payload
from altcha import verify_solution

from chatsbom.server.state import WebState

logger = structlog.get_logger('challenge')

#: The key derivation: PBKDF2 with SHA-256, which every browser's
#: WebCrypto runs natively, and which ALTCHA recommends where devices
#: vary and bundles with its widget.
ALGORITHM = 'PBKDF2/SHA-256'

#: PBKDF2's iterations for each key: ALTCHA's recommended cost.
COST = 5_000

#: The counter a client must reach is drawn from this range, both ends
#: included: about 1 to 2 s on a low-end phone.
#:
#: Set from ALTCHA's own benchmark (playground.altcha.org, About,
#: Benchmarks), in deterministic mode with 8 workers: PBKDF2/SHA-256 at
#: cost 5000 and counter 5000 took about 9,500 ms on a Samsung Galaxy
#: A14 in Chrome, which ran 4 threads, and about 1,600 ms on a MacBook
#: Pro (M3 Pro) in Chrome. The work is cost times counter, so a counter
#: of 500 to 1000 is a tenth to a fifth of that: 0.95 to 1.9 s on that
#: phone, and 0.16 to 0.32 s on that laptop. Not measured on a phone
#: here. ALTCHA recommends 5,000 to 10,000, 10 to 19 s on that phone.
COUNTERS = (500, 1_000)

#: How long a challenge may be solved and used in, from its issue.
TTL_SECONDS = 600

#: The longest payload read. A solved challenge is under a kilobyte.
MAX_PAYLOAD = 4_096

#: Hexadecimal digits: what every key, nonce and signature is written in.
HEX = re.compile(r'(?:[0-9a-fA-F]{2})*')


class Verdict(enum.Enum):
    """What verifying a solution came to."""

    VERIFIED = 'verified'
    #: Not a solved challenge at all.
    MALFORMED = 'malformed'
    #: Not signed by this service, or changed since it was.
    BAD_SIGNATURE = 'bad signature'
    #: Signed, but the key was not the one the challenge asked for.
    WRONG_SOLUTION = 'wrong solution'
    EXPIRED = 'expired'
    #: Issued to another client.
    OTHER_CLIENT = 'other client'
    #: Used before.
    REPLAYED = 'replayed'


def _subkey(key: bytes, purpose: bytes) -> bytes:
    """A key for one purpose, derived from the one configured
    (ALTCHA_HMAC_KEY): the challenge and the derived key are signed
    with different keys, so that neither signature can stand in for the
    other."""
    return hmac.new(key, b'chatsbom altcha: ' + purpose, hashlib.sha256).digest()


def _hex(value: object, *, empty: bool = False) -> TypeGuard[str]:
    """Whether `value` is bytes written in hex, and not none of them
    unless `empty` allows it."""
    return (
        isinstance(value, str)
        and (empty or value != '')
        and HEX.fullmatch(value) is not None
    )


def _whole(value: object) -> TypeGuard[int]:
    """Whether `value` is a whole number: JSON's `true` is not one."""
    return isinstance(value, int) and not isinstance(value, bool)


def _parse(text: str) -> Payload | None:
    """The solved challenge `text` holds, if it has the shape of one: a
    solution the widget sends, base64 of the challenge and what solves
    it."""
    if len(text) > MAX_PAYLOAD:
        return None
    try:
        document = json.loads(base64.b64decode(text, validate=True))
    except ValueError:
        # Not base64, not text, or not JSON: each is a ValueError.
        return None
    if not isinstance(document, dict):
        return None
    challenge, solution = document.get('challenge'), document.get('solution')
    if not (isinstance(challenge, dict) and isinstance(solution, dict)):
        return None
    parameters = challenge.get('parameters')
    if not isinstance(parameters, dict):
        return None
    signature = challenge.get('signature')
    data = parameters.get('data')
    counter = solution.get('counter')
    shaped = (
        # A signature left out, or empty, is a bad one: the library says
        # so. One of another kind would make it raise.
        (signature is None or _hex(signature, empty=True))
        and parameters.get('algorithm') == ALGORITHM
        and _hex(parameters.get('nonce'))
        and _hex(parameters.get('salt'))
        and _whole(parameters.get('expiresAt'))
        and isinstance(data, dict)
        and isinstance(data.get('client'), str)
        # The widget writes the counter as an unsigned 32-bit integer.
        and _whole(counter) and 0 <= counter < 2**32
        and _hex(solution.get('derivedKey'))
    )
    if not shaped:
        return None
    try:
        return Payload.from_dict(document)
    except (KeyError, TypeError, ValueError):
        return None


class Challenges:
    """Issues challenges signed with `key`, and verifies their solutions,
    keeping the used ones in `state`.

    `cost` and `counters` set the difficulty; the tests make it trivial.
    """

    def __init__(
        self,
        key: bytes,
        state: WebState,
        *,
        cost: int = COST,
        counters: tuple[int, int] = COUNTERS,
        ttl: float = TTL_SECONDS,
    ) -> None:
        low, high = counters
        if not 0 <= low <= high < 2**32:
            raise ValueError(f'not a range of counters: {counters}')
        self._signing = _subkey(key, b'challenge')
        self._keying = _subkey(key, b'derived key')
        self._state = state
        self.cost = cost
        self.counters = counters
        self.ttl = ttl

    def issue(self, client: str, *, now: float | None = None) -> dict[str, Any]:
        """A challenge for `client`, as the widget reads it: its
        parameters, and their signature. `now` is seconds since the
        epoch, now unless given."""
        now = time.time() if now is None else now
        low, high = self.counters
        challenge = create_challenge(
            ALGORITHM,
            self.cost,
            counter=low + secrets.randbelow(high - low + 1),
            expires_at=int(now + self.ttl),
            data={'client': client},
            hmac_secret=self._signing,
            hmac_key_secret=self._keying,
        )
        issued: dict[str, Any] = challenge.to_dict()
        return issued

    def verify(
        self, payload: str, client: str, *, now: float | None = None,
    ) -> Verdict:
        """Whether `payload` solves a challenge this service issued to
        `client`, unexpired at `now` and not used before; marked used
        if it does. Only a verified solution is marked: another client's
        attempt does not spend the challenge it was issued to."""
        now = time.time() if now is None else now
        parsed = _parse(payload)
        if parsed is None:
            return Verdict.MALFORMED
        try:
            result = verify_solution(
                parsed, self._signing, hmac_key_secret=self._keying,
            )
        except Exception as error:
            # Its input was shaped as a solution should be; a case the
            # shape does not cover is still a refusal, never a 500.
            logger.warning('challenge not verified', error=repr(error))
            return Verdict.MALFORMED
        if result.error:
            return Verdict.MALFORMED
        if result.expired:
            return Verdict.EXPIRED
        if result.invalid_signature:
            return Verdict.BAD_SIGNATURE
        if not result.verified:
            return Verdict.WRONG_SOLUTION

        # Signed by this service, so these are what it issued.
        parameters = parsed.challenge.parameters
        expires_at = parameters.expires_at
        if expires_at is None or expires_at <= now:
            return Verdict.EXPIRED
        if (parameters.data or {}).get('client') != client:
            return Verdict.OTHER_CLIENT
        if not self._first_use(parameters.nonce, expires_at):
            return Verdict.REPLAYED
        return Verdict.VERIFIED

    def forget_expired(self, *, now: float | None = None) -> int:
        """Forget the used challenges that have expired by `now`, and say
        how many: once expired, a challenge is refused for that alone."""
        now = time.time() if now is None else now
        with self._state.connect() as db:
            forgotten = db.execute(
                'DELETE FROM used_challenges WHERE expires_at <= ?', (now,),
            ).rowcount
        return int(forgotten)

    def _first_use(self, nonce: str, expires_at: int) -> bool:
        """Mark the challenge `nonce` names used, and say whether it was
        not already. One insert, so two uses at once cannot both be the
        first."""
        with self._state.connect() as db:
            inserted = db.execute(
                'INSERT OR IGNORE INTO used_challenges (nonce, expires_at) '
                'VALUES (?, ?)',
                (nonce.lower(), expires_at),
            ).rowcount
        return bool(inserted == 1)
