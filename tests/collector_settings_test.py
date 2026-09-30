"""The collector's settings: its GitHub tokens, and the reserve it leaves
in each of their buckets for manual work (#156).

Every token serves every bucket, with no split between them: GITHUB_TOKEN
and whatever CHATSBOM_GITHUB_TOKENS lists, in that order. A token is
cleaned as #117 cleans one, and one holding what no GitHub token holds
is refused by name and position, never shown.
"""
from datetime import timedelta

import pytest

from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.settings import DEFAULT_RESERVE
from chatsbom.collector.settings import DEFAULT_SWEEP_INTERVAL
from chatsbom.collector.settings import DEFAULT_UNIVERSE_INTERVAL
from chatsbom.collector.settings import settings_from
from chatsbom.collector.settings import SettingsError
from chatsbom.collector.tokens import scrub
from chatsbom.collector.tokens import Token

ONE = 'ghp_' + 'a' * 36
TWO = 'ghp_' + 'b' * 36
THREE = 'github_pat_' + 'c' * 82


class TestTheTokens:
    def test_github_token_is_token_1(self):
        settings = settings_from({'GITHUB_TOKEN': ONE})
        assert [token.label for token in settings.tokens] == ['token 1']
        assert settings.tokens[0].secret == ONE

    def test_more_follow_in_the_order_listed(self):
        settings = settings_from({
            'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_TOKENS': f'{TWO},{THREE}',
        })
        assert [token.secret for token in settings.tokens] == [ONE, TWO, THREE]
        assert [token.label for token in settings.tokens] == [
            'token 1', 'token 2', 'token 3',
        ]

    def test_the_list_is_split_at_commas_and_whitespace(self):
        """One token a line, as a file of them has it, or a list."""
        settings = settings_from({
            'CHATSBOM_GITHUB_TOKENS': f' {ONE},\n{TWO}\r\n , {THREE}\n',
        })
        assert [token.secret for token in settings.tokens] == [ONE, TWO, THREE]

    def test_the_list_alone_is_enough(self):
        settings = settings_from({
            'GITHUB_TOKEN': '', 'CHATSBOM_GITHUB_TOKENS': TWO,
        })
        assert [token.label for token in settings.tokens] == ['token 1']
        assert settings.tokens[0].secret == TWO

    def test_a_token_named_twice_is_one_token(self):
        """GitHub meters the token, not the listing: two of one would
        only be refused twice as fast."""
        settings = settings_from({
            'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_TOKENS': f'{TWO},{ONE},{TWO}',
        })
        assert [token.secret for token in settings.tokens] == [ONE, TWO]

    def test_the_whitespace_around_a_token_is_left_out(self):
        """A token read from a file ends with its line ending (#113)."""
        settings = settings_from({'GITHUB_TOKEN': f'  {ONE}\r\n'})
        assert settings.tokens[0].secret == ONE

    @pytest.mark.parametrize(
        'environ', [
            {},
            {'GITHUB_TOKEN': ''},
            {'GITHUB_TOKEN': '  \n', 'CHATSBOM_GITHUB_TOKENS': ' , ,\n'},
        ],
    )
    def test_none_at_all_is_refused(self, environ):
        with pytest.raises(SettingsError) as refused:
            settings_from(environ)
        assert refused.value.setting == 'GITHUB_TOKEN'
        assert 'CHATSBOM_GITHUB_TOKENS' in str(refused.value)

    @pytest.mark.parametrize('character', ['\x00', '\r', '\x1b', '\x7f', 'é', ' '])
    def test_github_token_holding_what_no_token_holds_is_refused(
        self, character,
    ):
        """#117's rule, where a request would have refused it quoting the
        header, token and all: the character and where it is, and never
        the token."""
        secret = f'ghp_abcdefg{character}hijklmnopqrstuvwxyz0123456789'
        with pytest.raises(SettingsError) as refused:
            settings_from({'GITHUB_TOKEN': secret})
        message = str(refused.value)
        assert refused.value.setting == 'GITHUB_TOKEN'
        assert f'U+{ord(character):04X}' in message
        assert 'character 12' in message
        assert 'abcdefg' not in message and 'hijklm' not in message

    def test_a_listed_token_holding_it_is_refused_by_its_place(self):
        secret = 'ghp_abcdefg\x00hijklmnopqrstuvwxyz0123456789'
        with pytest.raises(SettingsError) as refused:
            settings_from({
                'GITHUB_TOKEN': ONE,
                'CHATSBOM_GITHUB_TOKENS': f'{TWO},{secret}',
            })
        message = str(refused.value)
        assert refused.value.setting == 'CHATSBOM_GITHUB_TOKENS'
        assert 'U+0000' in message
        assert 'token 2 of CHATSBOM_GITHUB_TOKENS' in message
        assert 'abcdefg' not in message and TWO not in message

    def test_no_token_is_shown_by_the_settings(self):
        settings = settings_from({
            'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_TOKENS': TWO,
        })
        shown = ' '.join([
            repr(settings), str(settings), repr(settings.tokens),
            str(settings.tokens[0]), f'{settings.tokens[1]}',
            repr(settings.tokens[1]),
        ])
        assert ONE not in shown and TWO not in shown
        assert 'token 1' in shown and 'token 2' in shown


class TestScrubbing:
    def test_takes_every_token_out_of_a_text(self):
        tokens = [Token('token 1', ONE), Token('token 2', TWO)]
        text = f'GET /x: header Authorization: {ONE}, then {TWO}.'
        scrubbed = scrub(text, tokens)
        assert ONE not in scrubbed and TWO not in scrubbed
        assert scrubbed == 'GET /x: header Authorization: *****, then *****.'

    def test_takes_the_longest_first(self):
        """Or what one token leaves of another that begins with it would
        be shown."""
        longer = f'{ONE}xyz'
        tokens = [Token('token 1', ONE), Token('token 2', longer)]
        assert scrub(f'in {longer} here', tokens) == 'in ***** here'

    def test_takes_out_what_redact_does_too(self):
        text = (
            'https://example.com/r.json?X-Amz-Signature=5ec7e7 '
            'Bearer abcdefgh1'
        )
        assert scrub(text, []) == (
            'https://example.com/r.json?***** Bearer *****'
        )


class TestTheReserve:
    def test_is_the_default_unless_set(self):
        assert DEFAULT_RESERVE == {'core': 500, 'graphql': 500, 'search': 5}
        for value in (None, '', '  '):
            environ = {'GITHUB_TOKEN': ONE}
            if value is not None:
                environ['CHATSBOM_GITHUB_RESERVE'] = value
            assert settings_from(environ).reserve == DEFAULT_RESERVE

    def test_sets_the_buckets_it_names_and_keeps_the_others(self):
        settings = settings_from({
            'GITHUB_TOKEN': ONE,
            'CHATSBOM_GITHUB_RESERVE': 'core=100, dependency_sbom=10',
        })
        assert settings.reserve == {
            'core': 100, 'graphql': 500, 'search': 5, 'dependency_sbom': 10,
        }

    def test_zero_leaves_none(self):
        settings = settings_from({
            'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_RESERVE': 'search=0',
        })
        assert settings.reserve['search'] == 0

    @pytest.mark.parametrize(
        'value', [
            'core', 'core=', '=5', 'core=-1', 'core=ten', 'core=1.5',
            'core name=5', 'core=5;search=1',
        ],
    )
    def test_one_it_cannot_read_is_refused(self, value):
        with pytest.raises(SettingsError) as refused:
            settings_from({
                'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_RESERVE': value,
            })
        assert refused.value.setting == 'CHATSBOM_GITHUB_RESERVE'
        assert repr(value) in str(refused.value)


class TestTheIntervals:
    """How often the sweep asks after the universe, and how often the
    universe is searched again (#160)."""

    def test_are_an_hour_and_a_week_unless_set(self):
        assert DEFAULT_SWEEP_INTERVAL == timedelta(hours=1)
        assert DEFAULT_UNIVERSE_INTERVAL == timedelta(days=7)
        for value in (None, '', '  '):
            environ = {'GITHUB_TOKEN': ONE}
            if value is not None:
                environ['CHATSBOM_SWEEP_INTERVAL'] = value
                environ['CHATSBOM_UNIVERSE_INTERVAL'] = value
            settings = settings_from(environ)
            assert settings.sweep_interval == DEFAULT_SWEEP_INTERVAL
            assert settings.universe_interval == DEFAULT_UNIVERSE_INTERVAL

    @pytest.mark.parametrize(
        ('value', 'interval'), [
            ('90s', timedelta(seconds=90)),
            ('30m', timedelta(minutes=30)),
            ('2h', timedelta(hours=2)),
            ('14d', timedelta(days=14)),
            ('1w', timedelta(weeks=1)),
            (' 3H ', timedelta(hours=3)),
        ],
    )
    def test_are_a_whole_number_and_a_unit(self, value, interval):
        settings = settings_from({
            'GITHUB_TOKEN': ONE, 'CHATSBOM_SWEEP_INTERVAL': value,
            'CHATSBOM_UNIVERSE_INTERVAL': value,
        })
        assert settings.sweep_interval == interval
        assert settings.universe_interval == interval

    @pytest.mark.parametrize(
        'value', ['1', 'h', '0h', '-1h', '1.5h', '1 hour', '1h30m', '7x'],
    )
    @pytest.mark.parametrize(
        'setting', ['CHATSBOM_SWEEP_INTERVAL', 'CHATSBOM_UNIVERSE_INTERVAL'],
    )
    def test_one_it_cannot_read_is_refused(self, setting, value):
        with pytest.raises(SettingsError) as refused:
            settings_from({'GITHUB_TOKEN': ONE, setting: value})
        assert refused.value.setting == setting
        assert repr(value) in str(refused.value)


def test_the_settings_are_what_was_read():
    settings = settings_from({'GITHUB_TOKEN': ONE})
    assert isinstance(settings, CollectorSettings)
    assert settings == settings_from({'GITHUB_TOKEN': f' {ONE} '})
