"""The proof of work a question is to carry: ALTCHA, server side (#134).

Turnstile goes, since Cloudflare says it is not supported in mainland
China (#128, section 2.8, and the owner's decision on Q6). In its place
the service issues an HMAC-signed challenge, with its expiry and the
client's key among its signed parameters, which the page solves in a
Web Worker; and it verifies, once a question, the signature, the
solution, the expiry, the client and that the challenge was not used
before. Nothing calls the verification yet: the chat will.

A proof of work prices automation; it does not identify people. The
daily spend cap is the real bound.
"""
import base64
import json
import threading
import time
from typing import Any

import pytest
from altcha import Challenge
from altcha import Payload
from altcha import solve_challenge

from chatsbom.server.challenge import ALGORITHM
from chatsbom.server.challenge import Challenges
from chatsbom.server.challenge import COST
from chatsbom.server.challenge import COUNTERS
from chatsbom.server.challenge import TTL_SECONDS
from chatsbom.server.challenge import Verdict
from chatsbom.server.state import WebState

KEY = b'k' * 32
CLIENT = '203.0.113.7'


@pytest.fixture
def state(tmp_path) -> WebState:
    return WebState(tmp_path)


@pytest.fixture
def challenges(state: WebState) -> Challenges:
    """At a difficulty a test solves at once: a counter of 5 to 10, one
    PBKDF2 iteration each."""
    return Challenges(KEY, state, cost=1, counters=(5, 10))


def solved(issued: dict[str, Any]) -> str:
    """What the page sends back for `issued`, solved as its worker would."""
    challenge = Challenge.from_dict(issued)
    solution = solve_challenge(challenge, timeout=30)
    assert solution is not None
    return Payload(challenge, solution).to_base64()


def encode(document: object) -> str:
    return base64.b64encode(json.dumps(document).encode()).decode()


def decode(payload: str) -> dict[str, Any]:
    decoded: dict[str, Any] = json.loads(base64.b64decode(payload))
    return decoded


class TestIssuing:
    def test_signs_the_expiry_and_the_client_into_the_challenge(
        self, challenges,
    ):
        now = time.time()
        issued = challenges.issue(CLIENT, now=now)

        parameters = issued['parameters']
        assert parameters['algorithm'] == ALGORITHM
        assert parameters['expiresAt'] == int(now + TTL_SECONDS)
        assert parameters['data'] == {'client': CLIENT}
        # Signed, with the derived key's own signature beside it, so that
        # verifying needs no key derivation (deterministic mode).
        assert len(issued['signature']) == 64
        assert parameters['keySignature']

    def test_hides_the_counter_the_client_must_find(self, challenges):
        issued = challenges.issue(CLIENT)
        text = json.dumps(issued)
        assert 'counter' not in text
        solution = solve_challenge(Challenge.from_dict(issued))
        assert solution is not None
        assert 5 <= solution.counter <= 10

    def test_issues_a_new_challenge_each_time(self, challenges):
        first, second = challenges.issue(CLIENT), challenges.issue(CLIENT)
        assert first['parameters']['nonce'] != second['parameters']['nonce']
        assert first['parameters']['salt'] != second['parameters']['salt']


class TestTheDifficulty:
    """About 1-2 s on a phone, set from ALTCHA's own benchmark (see
    `challenge.COUNTERS`): PBKDF2/SHA-256 at cost 5000 and counter 5000
    took about 9,500 ms on a low-end Android, a Samsung Galaxy A14 in
    Chrome, and the work is linear in cost times counter."""

    def test_is_pbkdf2_at_altchas_cost(self):
        assert (ALGORITHM, COST) == ('PBKDF2/SHA-256', 5_000)

    def test_is_a_tenth_to_a_fifth_of_the_benchmarked_work(self):
        low, high = COUNTERS
        benchmark_ms = 9_500 * 1.0 / (5_000 * 5_000)
        assert (low, high) == (500, 1_000)
        assert round(COST * low * benchmark_ms) == 950
        assert round(COST * high * benchmark_ms) == 1_900

    def test_is_solved_and_verified_at_that_difficulty(self, state):
        """Once, at the real cost: in Python on one core, as a script
        would solve it, this is 0.8 to 1.5 s here."""
        real = Challenges(KEY, state)
        issued = real.issue(CLIENT)
        assert issued['parameters']['cost'] == COST
        started = time.monotonic()
        payload = solved(issued)
        assert time.monotonic() - started < 30
        assert real.verify(payload, CLIENT) is Verdict.VERIFIED


class TestVerifying:
    def test_takes_a_solution_from_the_client_it_was_issued_to(
        self, challenges,
    ):
        payload = solved(challenges.issue(CLIENT))
        assert challenges.verify(payload, CLIENT) is Verdict.VERIFIED

    def test_refuses_another_clients(self, challenges):
        """Solved by one, used by another: a farm that solves for many."""
        payload = solved(challenges.issue(CLIENT))
        assert challenges.verify(payload, '198.51.100.9') is (
            Verdict.OTHER_CLIENT
        )

    def test_refuses_one_used_before(self, challenges):
        payload = solved(challenges.issue(CLIENT))
        assert challenges.verify(payload, CLIENT) is Verdict.VERIFIED
        assert challenges.verify(payload, CLIENT) is Verdict.REPLAYED

    def test_refuses_one_used_before_a_restart(self, state):
        """The used challenges are kept in web.sqlite, not in memory."""
        before = Challenges(KEY, state, cost=1, counters=(5, 10))
        payload = solved(before.issue(CLIENT))
        assert before.verify(payload, CLIENT) is Verdict.VERIFIED

        after = Challenges(KEY, WebState(state.path.parent), cost=1)
        assert after.verify(payload, CLIENT) is Verdict.REPLAYED

    def test_lets_one_of_several_uses_at_once_through(self, challenges):
        payload = solved(challenges.issue(CLIENT))
        start = threading.Barrier(8)
        verdicts: list[Verdict] = []

        def use() -> None:
            start.wait()
            verdicts.append(challenges.verify(payload, CLIENT))

        threads = [threading.Thread(target=use) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(verdict.value for verdict in verdicts) == sorted(
            [Verdict.VERIFIED.value] + [Verdict.REPLAYED.value] * 7,
        )

    def test_refuses_one_that_has_expired(self, challenges):
        now = time.time()
        payload = solved(challenges.issue(CLIENT, now=now))
        later = now + TTL_SECONDS + 1
        assert challenges.verify(payload, CLIENT, now=later) is (
            Verdict.EXPIRED
        )

    def test_takes_one_just_before_it_expires(self, challenges):
        now = time.time()
        payload = solved(challenges.issue(CLIENT, now=now))
        assert challenges.verify(
            payload, CLIENT, now=now + TTL_SECONDS - 2,
        ) is Verdict.VERIFIED

    def test_refuses_a_wrong_solution(self, challenges):
        payload = decode(solved(challenges.issue(CLIENT)))
        key = payload['solution']['derivedKey']
        # Another key: what another counter would have given.
        payload['solution']['derivedKey'] = key[::-1]
        assert challenges.verify(encode(payload), CLIENT) is (
            Verdict.WRONG_SOLUTION
        )

    def test_refuses_a_guess_that_did_no_work(self, challenges):
        """The signed prefix is half the key; the other half is known
        only to whoever derived it."""
        issued = challenges.issue(CLIENT)
        guess = issued['parameters']['keyPrefix'] + '0' * 32
        payload = encode({
            'challenge': issued, 'solution': {'counter': 0, 'derivedKey': guess},
        })
        assert challenges.verify(payload, CLIENT) is Verdict.WRONG_SOLUTION

    @pytest.mark.parametrize(
        'field,value',
        [
            ('data', {'client': '198.51.100.9'}),
            ('expiresAt', 4_000_000_000),
            # The fixture's cost is 1.
            ('cost', 2),
            ('keyPrefix', '00'),
        ],
    )
    def test_refuses_a_challenge_altered_after_it_was_signed(
        self, challenges, field, value,
    ):
        """The client key, the expiry and the difficulty are signed:
        none can be changed to suit the client."""
        payload = decode(solved(challenges.issue(CLIENT)))
        payload['challenge']['parameters'][field] = value
        client = value['client'] if field == 'data' else CLIENT
        assert challenges.verify(encode(payload), client) is (
            Verdict.BAD_SIGNATURE
        )

    def test_refuses_a_challenge_signed_with_another_key(self, state):
        theirs = Challenges(b'x' * 32, state, cost=1, counters=(5, 10))
        ours = Challenges(KEY, state, cost=1, counters=(5, 10))
        payload = solved(theirs.issue(CLIENT))
        assert ours.verify(payload, CLIENT) is Verdict.BAD_SIGNATURE

    @pytest.mark.parametrize('signature', [None, ''])
    def test_refuses_a_challenge_with_no_signature(
        self, challenges, signature,
    ):
        payload = decode(solved(challenges.issue(CLIENT)))
        if signature is None:
            del payload['challenge']['signature']
        else:
            payload['challenge']['signature'] = signature
        assert challenges.verify(encode(payload), CLIENT) is (
            Verdict.BAD_SIGNATURE
        )

    def test_does_not_use_up_a_challenge_it_refuses(self, challenges):
        """Only a verified solution marks its challenge used: another
        client's attempt must not spend the one it was issued to."""
        payload = solved(challenges.issue(CLIENT))
        assert challenges.verify(payload, '198.51.100.9') is (
            Verdict.OTHER_CLIENT
        )
        assert challenges.verify(payload, CLIENT) is Verdict.VERIFIED


class TestMalformedPayloads:
    """Anything else is refused, and never raises: the library's own
    verification raises on some, a negative counter for one."""

    @pytest.mark.parametrize(
        'payload',
        [
            '',
            'not base64!',
            base64.b64encode(b'not json').decode(),
            base64.b64encode(b'\xff\xfe').decode(),
            encode([]),
            encode({}),
            encode({'challenge': None, 'solution': None, 'test': True}),
            'x' * 10_000,
        ],
        ids=[
            'empty', 'not base64', 'not json', 'not utf-8', 'a list',
            'an empty object', "the widget's test mode", 'too long',
        ],
    )
    def test_refuses(self, challenges, payload):
        assert challenges.verify(payload, CLIENT) is Verdict.MALFORMED

    @pytest.mark.parametrize(
        'path,value',
        [
            (('solution', 'counter'), -1),
            (('solution', 'counter'), 2**32),
            (('solution', 'counter'), '7'),
            (('solution', 'counter'), True),
            (('solution', 'derivedKey'), 'not hex'),
            (('solution', 'derivedKey'), 7),
            (('challenge', 'signature'), 'é' * 64),
            (('challenge', 'signature'), 7),
            (('challenge', 'parameters', 'expiresAt'), 'soon'),
            (('challenge', 'parameters', 'algorithm'), 'SHA-256'),
            (('challenge', 'parameters', 'data'), None),
            (('challenge', 'parameters', 'data'), {'client': 7}),
            (('challenge', 'parameters', 'nonce'), None),
        ],
        ids=lambda item: str(item),
    )
    def test_refuses_a_solved_payload_with_a_field_of_the_wrong_kind(
        self, challenges, path, value,
    ):
        payload = decode(solved(challenges.issue(CLIENT)))
        *parents, name = path
        target = payload
        for parent in parents:
            target = target[parent]
        target[name] = value
        assert challenges.verify(encode(payload), CLIENT) is (
            Verdict.MALFORMED
        )


class TestForgettingUsedChallenges:
    def test_forgets_those_past_their_expiry(self, challenges, state):
        now = time.time()
        first = solved(challenges.issue(CLIENT, now=now))
        second = solved(challenges.issue(CLIENT, now=now + 60))
        assert challenges.verify(first, CLIENT, now=now) is Verdict.VERIFIED
        assert challenges.verify(second, CLIENT, now=now) is Verdict.VERIFIED

        forgotten = challenges.forget_expired(now=now + TTL_SECONDS + 30)

        assert forgotten == 1
        # Past its expiry, a challenge is refused for that alone.
        assert challenges.verify(
            first, CLIENT, now=now + TTL_SECONDS + 30,
        ) is Verdict.EXPIRED
        assert challenges.verify(
            second, CLIENT, now=now + TTL_SECONDS + 30,
        ) is Verdict.REPLAYED
