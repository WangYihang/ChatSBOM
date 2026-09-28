"""`openapi stats` measures repositories against eleven context windows.

It asked litellm for them, and imported it for nothing else: importing
litellm fetched its model price list over the network, unless told not
to, and loaded a `.env` of its own, found by walking up from where it is
installed (#26).
"""
import sys

from chatsbom.commands.openapi import stats

#: What `get_context_windows` returned from litellm 1.81.16, offline
#: (LITELLM_LOCAL_MODEL_COST_MAP=True): each model's `max_input_tokens`,
#: and nine tenths of it, the most a repository may hold to fit.
LITELLM = {
    'GPT-5': {
        'limit': 115200, 'full_limit': 128000,
        'display': 'GPT-5 (128k)',
    },
    'Opus-4.6': {
        'limit': 900000, 'full_limit': 1000000,
        'display': 'Opus-4.6 (1000k)',
    },
    'Opus-4.5': {
        'limit': 180000, 'full_limit': 200000,
        'display': 'Opus-4.5 (200k)',
    },
    'Gemini-3.1-Pro': {
        'limit': 943718, 'full_limit': 1048576,
        'display': 'Gemini-3.1-Pro (1048k)',
    },
    'Gemini-3.0-Pro': {
        'limit': 943718, 'full_limit': 1048576,
        'display': 'Gemini-3.0-Pro (1048k)',
    },
    'DeepSeek-V3.2': {
        'limit': 147456, 'full_limit': 163840,
        'display': 'DeepSeek-V3.2 (163k)',
    },
    'DeepSeek-R1': {
        'limit': 58982, 'full_limit': 65536,
        'display': 'DeepSeek-R1 (65k)',
    },
    'Llama-4-Scout': {
        'limit': 117964, 'full_limit': 131072,
        'display': 'Llama-4-Scout (131k)',
    },
    'Qwen-3': {
        'limit': 115200, 'full_limit': 128000,
        'display': 'Qwen-3 (128k)',
    },
    'GLM-4.7': {
        'limit': 180000, 'full_limit': 200000,
        'display': 'GLM-4.7 (200k)',
    },
    'Kimi-k2.5': {
        'limit': 235929, 'full_limit': 262144,
        'display': 'Kimi-k2.5 (262k)',
    },
}


def test_the_context_windows_are_the_ones_litellm_gave():
    assert stats.get_context_windows() == LITELLM


def test_the_context_windows_need_no_litellm(monkeypatch):
    """As if it were not installed: nothing else used it."""
    monkeypatch.setitem(sys.modules, 'litellm', None)

    assert stats.get_context_windows() == LITELLM
