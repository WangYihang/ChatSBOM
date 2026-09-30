"""Asking GitHub again, after a pause, what it failed to answer (#160).

A server error, an answer that never came, or one that is not what was
asked for, often passes within minutes. The universe's search, about 700
requests listed whole or not at all, and the sweep, about 650 calls an
hour, each ask again rather than give up at the first failure.

A refusal for a rate limit is not a failure: the budget backs its bucket
off, and the client asks again (`client`). Nor is a token GitHub does
not take (`Unauthorized`), which no pause mends.
"""
import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeVar

import structlog

from chatsbom.collector.errors import Failed
from chatsbom.collector.errors import Unauthorized

logger = structlog.get_logger('collector.retry')

#: Each request is asked this many times at most.
ATTEMPTS = 3

#: Seconds after the first failure, and twice as long after each later
#: one: a minute, then two.
PAUSE = 60.0

Answer = TypeVar('Answer')


async def retried(
    ask: Callable[[], Awaitable[Answer]],
    *,
    what: str,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    again: Callable[[Answer], bool] | None = None,
) -> Answer:
    """What `ask` gives: asked again after a pause while it fails, or
    while `again` says so of what it gave, `ATTEMPTS` times in all. The
    last failure is raised, and the last answer given, whatever `again`
    says of it. `what` names it in the log."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            answer = await ask()
        except Unauthorized:
            raise
        except Failed as error:
            if attempt == ATTEMPTS:
                raise
            logger.warning(
                'GitHub failed: asking again after a pause', what=what,
                attempt=attempt, error=str(error),
            )
        else:
            if again is None or attempt == ATTEMPTS or not again(answer):
                return answer
            logger.info(
                'GitHub gave less than was asked: asking again after a '
                'pause', what=what, attempt=attempt,
            )
        await sleep(PAUSE * 2 ** (attempt - 1))
    raise AssertionError('every attempt returns or raises')
